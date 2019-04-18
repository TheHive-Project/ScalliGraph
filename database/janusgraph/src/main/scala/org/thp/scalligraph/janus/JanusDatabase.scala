package org.thp.scalligraph.janus

import java.util.{Date, UUID}

import scala.util.{Failure, Success, Try}

import play.api.{Configuration, Environment}

import gremlin.scala._
import javax.inject.{Inject, Singleton}
import org.apache.tinkerpop.gremlin.structure.{Edge ⇒ _, Element ⇒ _, Graph ⇒ _, Vertex ⇒ _}
import org.janusgraph.core._
import org.janusgraph.core.schema.{ConsistencyModifier, JanusGraphManagement, JanusGraphSchemaType, Mapping}
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.slf4j.MDC
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._

@Singleton
class JanusDatabase(graph: JanusGraph, maxRetryOnConflict: Int, override val chunkSize: Int) extends BaseDatabase {
  val name = "janus"

  @Inject() def this(configuration: Configuration) = {
    this(
      JanusGraphFactory.open(new Config(configuration.get[Configuration]("db.janusgraph"))),
      configuration.get[Int]("db.maxRetryOnConflict"),
      configuration.underlying.getBytes("db.chunkSize").toInt
    )
    logger.info(s"Instantiate JanusDatabase using ${configuration.get[String]("db.janusgraph.storage.backend")} backend")
  }

  def this() = this(Configuration.load(Environment.simple()))

  override def noTransaction[A](body: Graph ⇒ A): A = {
    logger.debug(s"Begin of no-transaction")
    val a = body(graph)
    logger.debug(s"End of no-transaction")
    a
  }

  override def tryTransaction[A](body: Graph ⇒ Try[A]): Try[A] =
    Retry(maxRetryOnConflict, classOf[PermanentLockingException], classOf[SchemaViolationException]) {
      //    val tx = graph.tx()
      //    tx.open() /*.createThreadedTx[JanusGraphTransaction]()*/
      // Transaction is automatically open at the first operation.
      val tx = graph.tx.createThreadedTx[JanusGraphTransaction]()
      MDC.put("tx", f"${tx.hashCode()}%08x")
      logger.debug("Begin of transaction")
      Try {
        val a = body(tx)
        tx.commit()
        logger.debug("End of transaction")
        MDC.remove("tx")
        a
      }.flatten
        .recoverWith {
          case t: PermanentLockingException ⇒ Failure(new DatabaseException(cause = t))
          case t: SchemaViolationException  ⇒ Failure(new DatabaseException(cause = t))
          case e: Throwable ⇒
            logger.error(s"Exception raised, rollback (${e.getMessage})")
            Try(tx.rollback())
            MDC.remove("tx")
            Failure(e)
        }
    }

  // FIXME "isAssignableFrom" can be replaced by "==" because classes are final.
  def convertToJava(c: Class[_]): Class[_] =
    if (classOf[Int].isAssignableFrom(c)) classOf[java.lang.Integer]
    else if (classOf[Double].isAssignableFrom(c)) classOf[java.lang.Double]
    else if (classOf[Float].isAssignableFrom(c)) classOf[java.lang.Float]
    else if (classOf[Char].isAssignableFrom(c)) classOf[java.lang.Character]
    else if (classOf[Long].isAssignableFrom(c)) classOf[java.lang.Long]
    else if (classOf[Short].isAssignableFrom(c)) classOf[java.lang.Short]
    else if (classOf[Byte].isAssignableFrom(c)) classOf[java.lang.Byte]
    else if (classOf[Boolean].isAssignableFrom(c)) classOf[java.lang.Boolean]
    else c

  private def createEntityProperties(mgmt: JanusGraphManagement): Unit = {
    mgmt
      .makePropertyKey("_id")
      .dataType(convertToJava(classOf[String]))
      .cardinality(Cardinality.SINGLE)
      .make()
    mgmt
      .makePropertyKey("_label")
      .dataType(convertToJava(classOf[String]))
      .cardinality(Cardinality.SINGLE)
      .make()
    mgmt
      .makePropertyKey("_createdBy")
      .dataType(convertToJava(classOf[String]))
      .cardinality(Cardinality.SINGLE)
      .make()
    mgmt
      .makePropertyKey("_createdAt")
      .dataType(convertToJava(classOf[Date]))
      .cardinality(Cardinality.SINGLE)
      .make()
    mgmt
      .makePropertyKey("_updatedBy")
      .dataType(convertToJava(classOf[String]))
      .cardinality(Cardinality.SINGLE)
      .make()
    mgmt
      .makePropertyKey("_updatedAt")
      .dataType(convertToJava(classOf[Date]))
      .cardinality(Cardinality.SINGLE)
      .make()
    ()
  }

  private def createElementLabels(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
    models.foreach {
      case m: VertexModel ⇒
        logger.trace(s"mgmt.getOrCreateVertexLabel(${m.label})")
        mgmt.getOrCreateVertexLabel(m.label)
      case m: EdgeModel[_, _] ⇒
        logger.trace(s"mgmt.getOrCreateEdgeLabel(${m.label})")
        mgmt.getOrCreateEdgeLabel(m.label)
    }

  private def createProperties(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
    models
      .flatMap(model ⇒ model.fields.map(f ⇒ (f._1, model, f._2)))
      .groupBy(_._1)
      .map {
        case (fieldName, mappings) ⇒
          val firstMapping = mappings.head._3
          if (!mappings.tail.forall(_._3 isCompatibleWith firstMapping)) {
            val msg = mappings.map {
              case (_, model, mapping) ⇒ s"  in model ${model.label}: ${mapping.graphTypeClass} (${mapping.cardinality})"
            }
            throw InternalError(s"Mapping of `$fieldName` has incompatible types:\n${msg.mkString("\n")}")
          }
          fieldName → firstMapping
      }
      .foreach {
        case (fieldName, mapping) ⇒
          logger.debug(s"Create property $fieldName of type ${mapping.graphTypeClass} (${mapping.cardinality})")
          val cardinality = mapping.cardinality match {
            case MappingCardinality.single ⇒ Cardinality.SINGLE
            case MappingCardinality.option ⇒ Cardinality.SINGLE
            case MappingCardinality.list   ⇒ Cardinality.LIST
            case MappingCardinality.set    ⇒ Cardinality.SET
          }
          logger.trace(
            s"mgmt.makePropertyKey($fieldName).dataType(${convertToJava(mapping.graphTypeClass).getSimpleName}.class).cardinality($cardinality).make()")
          mgmt
            .makePropertyKey(fieldName)
            .dataType(convertToJava(mapping.graphTypeClass))
            .cardinality(cardinality)
            .make()

      }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexType: IndexType.Value,
      properties: Seq[String]): Unit = {
    val indexName = elementLabel.name + "_" + properties.mkString("_")
    val index     = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
    val propertyKeys = (properties :+ "_label").map { p ⇒
      Option(mgmt.getPropertyKey(p)).getOrElse(throw InternalError(s"Property $p in ${elementLabel.name} not found"))
    }
    indexType match {
      case IndexType.unique ⇒
        logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.mkString(",")}")
        propertyKeys.foreach(index.addKey)
        index.unique()
        val i = index.buildCompositeIndex()
        mgmt.setConsistency(i, ConsistencyModifier.LOCK)
      case IndexType.standard ⇒
        logger.debug(s"Creating index on fields $elementLabel:${propertyKeys.mkString(",")}")
        propertyKeys.foreach(index.addKey)
        index.buildCompositeIndex()
      case IndexType.fulltext ⇒
        logger.debug(s"Creating fulltext index on fields $elementLabel:${propertyKeys.mkString(",")}")
        propertyKeys.foreach(k ⇒ index.addKey(k, Mapping.TEXT.asParameter()))
        index.buildMixedIndex("search")
    }
    ()
  }

  override def createSchema(models: Seq[Model]): Try[Unit] =
    Retry(maxRetryOnConflict, classOf[PermanentLockingException]) {
      logger.info("Creating database schema")
      graph.synchronized {
        val mgmt = graph.openManagement()
        val alreadyExists = models
          .map(_.label)
          .flatMap(l ⇒ Option(mgmt.getVertexLabel(l)).orElse(Option(mgmt.getEdgeLabel(l))))
          .map(_.toString)
        if (alreadyExists.nonEmpty) {
          logger.info(s"Models already exists. Skipping schema creation (existing labels: ${alreadyExists.mkString(",")})")
//          mgmt.rollback()
        } else {
          //    mgmt.setConsistency(leadidCUniqueIndex, ConsistencyModifier.LOCK)
          createElementLabels(mgmt, models)
          createEntityProperties(mgmt)
          createProperties(mgmt, models)
          logger.debug("Creating unique index on fields _id")
          mgmt
            .buildIndex("_id_vertex_index", classOf[Vertex])
            .addKey(mgmt.getPropertyKey("_id"))
            .unique()
            .buildCompositeIndex()
//            mgmt
//              .buildIndex("_id_edge_index", classOf[Edge])
//              .addKey(mgmt.getPropertyKey("_id"))
//              .unique()
//              .buildCompositeIndex()
          mgmt
            .buildIndex("_label_vertex_index", classOf[Vertex])
            .addKey(mgmt.getPropertyKey("_label"))
            .buildCompositeIndex()
          mgmt
            .buildIndex("_label_edge_index", classOf[Edge])
            .addKey(mgmt.getPropertyKey("_label"))
            .buildCompositeIndex()

          for {
            model                   ← models
            (indexType, properties) ← model.indexes
          } model match {
            case _: VertexModel     ⇒ createIndex(mgmt, classOf[Vertex], mgmt.getVertexLabel(model.label), indexType, properties)
            case _: EdgeModel[_, _] ⇒ createIndex(mgmt, classOf[Edge], mgmt.getEdgeLabel(model.label), indexType, properties)
          }
          // TODO add index for labels when it will be possible
          // cf. https://github.com/JanusGraph/janusgraph/issues/283
          ()
          mgmt.commit()
        }
        Success(())
      }
    }

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(this, graph)
    setSingleProperty(createdVertex, "_id", UUID.randomUUID, idMapping)
    setSingleProperty(createdVertex, "_createdAt", new Date, createdAtMapping)
    setSingleProperty(createdVertex, "_createdBy", authContext.userId, createdByMapping)
    setSingleProperty(createdVertex, "_label", model.label, UniMapping.stringMapping)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.toDomain(createdVertex)(this)
  }

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity): E with Entity = {
    val edgeMaybe = for {
      f ← graph.V().has(Key("_id") of from._id).headOption()
      t ← graph.V().has(Key("_id") of to._id).headOption()
    } yield {
      val createdEdge = model.create(e, f, t)(this, graph)
      setSingleProperty(createdEdge, "_id", UUID.randomUUID, idMapping)
      setSingleProperty(createdEdge, "_createdAt", new Date, createdAtMapping)
      setSingleProperty(createdEdge, "_createdBy", authContext.userId, createdByMapping)
      setSingleProperty(createdEdge, "_label", model.label, UniMapping.stringMapping)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.toDomain(createdEdge)(this)
    }
    edgeMaybe.getOrElse {
      val error = graph.V().has(Key("_id") of from._id).headOption().map(_ ⇒ "").getOrElse(s"${from._model.label}:${from._id} not found ") +
        graph.V().has(Key("_id") of to._id).headOption().map(_ ⇒ "").getOrElse(s"${to._model.label}:${to._id} not found")
      sys.error(s"Fail to create edge between ${from._model.label}:${from._id} and ${to._model.label}:${to._id}, $error")
    }
  }

  override def vertexStep(graph: Graph, model: Model): GremlinScala[Vertex] = graph.V.has(Key("_label") of model.label)
  override def edgeStep(graph: Graph, model: Model): GremlinScala[Edge]     = graph.E.has(Key("_label") of model.label)

  override def drop(): Unit = JanusGraphFactory.drop(graph)
}
