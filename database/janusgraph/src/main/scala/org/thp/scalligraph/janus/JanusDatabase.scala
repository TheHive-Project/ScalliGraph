package org.thp.scalligraph.janus

import java.util.Date

import scala.util.Try

import play.api.Configuration

import gremlin.scala._
import javax.inject.Singleton
import org.apache.tinkerpop.gremlin.structure.{Edge ⇒ _, Element ⇒ _, Graph ⇒ _, Vertex ⇒ _}
import org.janusgraph.core.schema.{ConsistencyModifier, JanusGraphManagement, JanusGraphSchemaType, Mapping}
import org.janusgraph.core.{Cardinality, JanusGraph, JanusGraphFactory, SchemaViolationException}
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration
import org.thp.scalligraph.models._
import org.thp.scalligraph.{Config, InternalError, Retry}

@Singleton
class JanusDatabase(graph: JanusGraph, maxRetryOnConflict: Int, override val chunkSize: Int) extends BaseDatabase {
  val name = "janus"

  def this(configuration: Configuration) =
    this(
      JanusGraphFactory.open(new Config(configuration)),
      configuration.getOptional[Int]("db.maxRetryOnConflict").getOrElse(5),
      configuration.getOptional[Int]("db.chunkSize").getOrElse(32 * 1024)
    )

  def this() = this(
    JanusGraphFactory.open(
      GraphDatabaseConfiguration.buildGraphConfiguration
        .set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory")
        //      .set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ZERO)
        .set(GraphDatabaseConfiguration.DROP_ON_CLEAR, new java.lang.Boolean(false))),
    5,
    32 * 1024
  )

  override def noTransaction[A](body: Graph ⇒ A): A = body(graph)

  override def transaction[A](body: Graph ⇒ A): A = Retry(maxRetryOnConflict, classOf[PermanentLockingException], classOf[SchemaViolationException]) {
    logger.debug(s"Begin of transaction")
    //    val tx = graph.tx()
    //    tx.open() /*.createThreadedTx[JanusGraphTransaction]()*/
    // Transaction is automatically open at the first operation.
    try {
      val a = body(graph)
      graph.tx.commit()
      logger.debug(s"End of transaction")
      a
    } catch {
      case e: Throwable ⇒
        logger.error(s"Exception raised, rollback (${e.getMessage})")
        Try(graph.tx.rollback())
        throw e
    }
  }

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
    Option(mgmt.getPropertyKey("_id")).getOrElse {
      mgmt
        .makePropertyKey("_id")
        .dataType(convertToJava(classOf[String]))
        .cardinality(Cardinality.SINGLE)
        .make()
    }
    Option(mgmt.getPropertyKey("_createdBy")).getOrElse {
      mgmt
        .makePropertyKey("_createdBy")
        .dataType(convertToJava(classOf[String]))
        .cardinality(Cardinality.SINGLE)
        .make()
    }
    Option(mgmt.getPropertyKey("_createdAt")).getOrElse {
      mgmt
        .makePropertyKey("_createdAt")
        .dataType(convertToJava(classOf[Date]))
        .cardinality(Cardinality.SINGLE)
        .make()
    }
    Option(mgmt.getPropertyKey("_updatedBy")).getOrElse {
      mgmt
        .makePropertyKey("_updatedBy")
        .dataType(convertToJava(classOf[String]))
        .cardinality(Cardinality.SINGLE)
        .make()
    }
    Option(mgmt.getPropertyKey("_updatedAt")).getOrElse {
      mgmt
        .makePropertyKey("_updatedAt")
        .dataType(convertToJava(classOf[Date]))
        .cardinality(Cardinality.SINGLE)
        .make()
    }
    ()
  }

  private def createElementLabels(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
    models.foreach {
      case m: VertexModel     ⇒ mgmt.getOrCreateVertexLabel(m.label)
      case m: EdgeModel[_, _] ⇒ mgmt.getOrCreateEdgeLabel(m.label)
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
          Option(mgmt.getPropertyKey(fieldName)).getOrElse {
            mgmt
              .makePropertyKey(fieldName)
              .dataType(convertToJava(mapping.graphTypeClass))
              .cardinality(cardinality)
              .make()
          }
      }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexType: IndexType.Value,
      properties: Seq[String]): Unit = {
    val indexName = elementLabel.name + "_" + properties.mkString("_")
    Option(mgmt.getGraphIndex(indexName)).getOrElse {
      val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
      val propertyKeys = properties.map { p ⇒
        Option(mgmt.getPropertyKey(p)).getOrElse(throw InternalError(s"Property $p in ${elementLabel.name} not found"))
      }
      indexType match {
        case IndexType.unique ⇒
          logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.map(_.label()).mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.unique()
          val i = index.buildCompositeIndex()
          mgmt.setConsistency(i, ConsistencyModifier.LOCK)
        case IndexType.standard ⇒
          logger.debug(s"Creating index on fields $elementLabel:${propertyKeys.map(_.label()).mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.buildCompositeIndex()
        case IndexType.fulltext ⇒
          logger.debug(s"Creating fulltext index on fields $elementLabel:${propertyKeys.map(_.label()).mkString(",")}")
          propertyKeys.foreach(k ⇒ index.addKey(k, Mapping.TEXT.asParameter()))
          index.buildMixedIndex("search")
      }
    }
    ()
  }

  override def createSchema(models: Seq[Model]): Unit =
    Retry(maxRetryOnConflict, classOf[PermanentLockingException]) {
      graph.synchronized {
        val mgmt = graph.openManagement()
        val alreadyExists = models
          .map(_.label)
          .flatMap(l ⇒ Option(mgmt.getVertexLabel(l)).orElse(Option(mgmt.getEdgeLabel(l))))
          .map(_.label)
        if (alreadyExists.nonEmpty) {
          logger.info(s"Models already exists. Skipping schema creation (existing labels: ${alreadyExists.mkString(",")})")
          mgmt.rollback()
        } else {
          //    mgmt.setConsistency(leadidCUniqueIndex, ConsistencyModifier.LOCK)
          createEntityProperties(mgmt)
          createElementLabels(mgmt, models)
          createProperties(mgmt, models)

          Option(mgmt.getGraphIndex("_id_vertex_index")).getOrElse {
            logger.debug("Creating unique index on fields _id")
            mgmt
              .buildIndex("_id_vertex_index", classOf[Vertex])
              .addKey(mgmt.getPropertyKey("_id"))
              .unique()
              .buildCompositeIndex()
          }
          Option(mgmt.getGraphIndex("_id_edge_index")).getOrElse {
            mgmt
              .buildIndex("_id_edge_index", classOf[Vertex])
              .addKey(mgmt.getPropertyKey("_id"))
              .unique()
              .buildCompositeIndex()
          }

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
      }
    }
}
