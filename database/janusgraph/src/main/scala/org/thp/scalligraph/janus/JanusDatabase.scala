package org.thp.scalligraph.janus

import java.nio.file.{Files, Paths}
import java.util.function.Consumer
import java.util.{Date, Properties}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import play.api.{Configuration, Environment}

import akka.actor.ActorSystem
import com.typesafe.config.ConfigObject
import gremlin.scala.{asScalaGraph, Edge, Element, Graph, GremlinScala, Key, Vertex}
import javax.inject.{Inject, Singleton}
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR
import org.janusgraph.core.schema.{ConsistencyModifier, JanusGraphManagement, JanusGraphSchemaType, Mapping}
import org.janusgraph.core.{Cardinality, JanusGraph, JanusGraphFactory, SchemaViolationException}
import org.janusgraph.diskstorage.PermanentBackendException
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.slf4j.MDC
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.utils.{Config, Retry}
import org.thp.scalligraph.InternalError

object JanusDatabase {

  def openDatabase(configuration: Configuration): JanusGraph = {
    val backend = configuration.get[String]("db.janusgraph.storage.backend")
    if (backend == "berkeleyje") {
      val jePropertyFile = Paths.get(configuration.get[String]("db.janusgraph.storage.directory"), "je.properties")
      configuration.getOptional[ConfigObject]("db.janusgraph.berkeleyje").foreach { configObject =>
        Files.createDirectories(jePropertyFile.getParent)
        val props = new Properties
        configObject.asScala.foreach { case (k, v) => props.put(s"je.$k", v.render()) }
        val propertyOutputStream = Files.newOutputStream(jePropertyFile)
        try props.store(propertyOutputStream, "DO NOT EDIT, FILE GENERATED FROM application.conf")
        finally propertyOutputStream.close()
      }
    }
    JanusGraphFactory.open(new Config(configuration.get[Configuration]("db.janusgraph")))
  }
}

@Singleton
class JanusDatabase(
    janusGraph: JanusGraph,
    maxAttempts: Int,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    override val chunkSize: Int,
    system: ActorSystem
) extends BaseDatabase {
  val name = "janus"

  val localTransaction: ThreadLocal[Option[Transaction]] = ThreadLocal.withInitial[Option[Transaction]](() => None)
  janusGraph.tx.onReadWrite(READ_WRITE_BEHAVIOR.MANUAL)

  @Inject() def this(configuration: Configuration, system: ActorSystem) = {
    this(
      JanusDatabase.openDatabase(configuration),
      configuration.get[Int]("db.onConflict.maxAttempts"),
      configuration.get[FiniteDuration]("db.onConflict.minBackoff"),
      configuration.get[FiniteDuration]("db.onConflict.maxBackoff"),
      configuration.get[Double]("db.onConflict.randomFactor"),
      configuration.underlying.getBytes("db.chunkSize").toInt,
      system
    )
    logger.info(s"Instantiate JanusDatabase using ${configuration.get[String]("db.janusgraph.storage.backend")} backend")
  }

  def this(system: ActorSystem) = this(Configuration.load(Environment.simple()), system)

  def isValidId(id: String): Boolean = id.forall(_.isDigit)

  override def roTransaction[R](body: Graph => R): R = {
    val graph = janusGraph.buildTransaction().readOnly().start()
    val tx    = graph.tx()
    val oldTx = localTransaction.get()
    if (oldTx.nonEmpty)
      logger.warn("Creating transaction while another transaction is already open")
    localTransaction.set(Some(tx))
    MDC.put("tx", f"${System.identityHashCode(tx)}%08x")
    logger.debug(s"Begin of readonly transaction")
    val r = body(graph)
    logger.debug(s"End of readonly transaction")
    MDC.remove("tx")
    localTransaction.set(oldTx)
    tx.commit()

    r
  }

  override def tryTransaction[R](body: Graph => Try[R]): Try[R] = {
    def executeCallbacks(graph: Graph): R => Try[R] = { r =>
      val currentCallbacks = takeCallbacks(graph)
      currentCallbacks
        .foldRight[Try[Unit]](Success(()))((cb, a) => a.flatMap(_ => cb()))
        .map(_ => r)
    }

    def commitTransaction(tx: Transaction): R => R = { r =>
      logger.debug("Committing transaction")
      tx.commit()
      logger.debug("End of transaction")
      r
    }

    def rollbackTransaction(tx: Transaction): PartialFunction[Throwable, Failure[R]] = {
      case t =>
        Try(tx.rollback())
        Failure(t)
    }
    val oldTx = localTransaction.get()
    if (oldTx.isDefined)
      logger.warn("Creating transaction while another transaction is already open")
    val result =
      Retry(maxAttempts)
        .on[DatabaseException]
        .on[SchemaViolationException]
        .on[PermanentBackendException]
        .on[PermanentLockingException]
        .withBackoff(minBackoff, maxBackoff, randomFactor)(system)
        .withTry {
          val graph = janusGraph.buildTransaction().start()
          val tx    = graph.tx()
          localTransaction.set(Some(tx))
          MDC.put("tx", f"${System.identityHashCode(tx)}%08x")
          logger.debug("Begin of transaction")
          Try(body(graph))
            .flatten
            .flatMap(executeCallbacks(graph))
            .map(commitTransaction(tx))
            .recoverWith(rollbackTransaction(tx))
        }
        .recoverWith {
          case t: PermanentLockingException => Failure(new DatabaseException(cause = t))
          case t: SchemaViolationException  => Failure(new DatabaseException(cause = t))
          case t: PermanentBackendException => Failure(new DatabaseException(cause = t))
          case e: Throwable =>
            logger.error(s"Exception raised, rollback (${e.getMessage})")
            Failure(e)
        }
    localTransaction.set(oldTx)
    MDC.remove("tx")
    result
  }

  def currentTransactionId(graph: Graph): AnyRef = graph

  override def addTransactionListener(listener: Consumer[Transaction.Status])(implicit graph: Graph): Unit =
    localTransaction.get() match {
      case Some(tx) => tx.addTransactionListener(listener)
      case None     => logger.warn("Trying to add a transaction listener without open transaction")
    }

  private def createEntityProperties(mgmt: JanusGraphManagement): Unit =
    if (Option(mgmt.getPropertyKey("_label")).isEmpty) {
      mgmt
        .makePropertyKey("_label")
        .dataType(classOf[String])
        .cardinality(Cardinality.SINGLE)
        .make()
      mgmt
        .makePropertyKey("_createdBy")
        .dataType(classOf[String])
        .cardinality(Cardinality.SINGLE)
        .make()
      mgmt
        .makePropertyKey("_createdAt")
        .dataType(classOf[Date])
        .cardinality(Cardinality.SINGLE)
        .make()
      mgmt
        .makePropertyKey("_updatedBy")
        .dataType(classOf[String])
        .cardinality(Cardinality.SINGLE)
        .make()
      mgmt
        .makePropertyKey("_updatedAt")
        .dataType(classOf[Date])
        .cardinality(Cardinality.SINGLE)
        .make()
      ()
    }

  private def createElementLabels(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
    models.foreach {
      case m: VertexModel if Option(mgmt.getVertexLabel(m.label)).isEmpty =>
        logger.trace(s"mgmt.getOrCreateVertexLabel(${m.label})")
        mgmt.getOrCreateVertexLabel(m.label)
      case m: EdgeModel[_, _] if Option(mgmt.getEdgeLabel(m.label)).isEmpty =>
        logger.trace(s"mgmt.getOrCreateEdgeLabel(${m.label})")
        mgmt.getOrCreateEdgeLabel(m.label)
      case m =>
        logger.info(s"Model ${m.label} already exists, ignore it")
    }

  private def createProperties(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
    models
      .flatMap(model => model.fields.map(f => (f._1, model, f._2)))
      .groupBy(_._1)
      .map {
        case (fieldName, mappings) =>
          val firstMapping = mappings.head._3
          if (!mappings.tail.forall(_._3 isCompatibleWith firstMapping)) {
            val msg = mappings.map {
              case (_, model, mapping) => s"  in model ${model.label}: ${mapping.graphTypeClass} (${mapping.cardinality})"
            }
            throw InternalError(s"Mapping of `$fieldName` has incompatible types:\n${msg.mkString("\n")}")
          }
          fieldName -> firstMapping
      }
      .foreach {
        case (fieldName, mapping) =>
          logger.debug(s"Create property $fieldName of type ${mapping.graphTypeClass} (${mapping.cardinality})")
          val cardinality = mapping.cardinality match {
            case MappingCardinality.single => Cardinality.SINGLE
            case MappingCardinality.option => Cardinality.SINGLE
            case MappingCardinality.list   => Cardinality.LIST
            case MappingCardinality.set    => Cardinality.SET
          }
          logger.trace(s"mgmt.makePropertyKey($fieldName).dataType(${mapping.graphTypeClass.getSimpleName}.class).cardinality($cardinality).make()")
          Option(mgmt.getPropertyKey(fieldName)) match {
            case None =>
              mgmt
                .makePropertyKey(fieldName)
                .dataType(mapping.graphTypeClass)
                .cardinality(cardinality)
                .make()
            case Some(p) =>
              if (p.dataType() == mapping.graphTypeClass && p.cardinality() == cardinality)
                logger.info(s"Property $fieldName $cardinality:${mapping.graphTypeClass} already exists, ignore it")
              else
                logger.error(
                  s"Property $fieldName exists with incompatible type: $cardinality:${mapping.graphTypeClass} Vs ${p.cardinality()}:${p.dataType()}"
                )
          }
      }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexType: IndexType.Value,
      properties: Seq[String]
  ): Unit = {
    val indexName = elementLabel.name + "_" + properties.mkString("_")
    if (Option(mgmt.getGraphIndex(indexName)).isDefined) {
      logger.info(s"Index $indexName already exists, ignore it")
    } else {
      val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
      val propertyKeys = (properties :+ "_label").map { p =>
        Option(mgmt.getPropertyKey(p)).getOrElse(throw InternalError(s"Property $p in ${elementLabel.name} not found"))
      }
      indexType match {
        case IndexType.unique =>
          logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.unique()
          val i = index.buildCompositeIndex()
          mgmt.setConsistency(i, ConsistencyModifier.LOCK)
        case IndexType.standard =>
          logger.debug(s"Creating index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.buildCompositeIndex()
        case IndexType.fulltext =>
          logger.debug(s"Creating fulltext index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(k => index.addKey(k, Mapping.TEXT.asParameter()))
          index.buildMixedIndex("search")
      }
      ()
    }
  }

  override def createSchema(models: Seq[Model]): Try[Unit] =
    Retry(maxAttempts)
      .on[PermanentLockingException]
      .withTry {
        logger.info("Creating database schema")
        janusGraph.synchronized {
          val mgmt = janusGraph.openManagement()
          createElementLabels(mgmt, models)
          createEntityProperties(mgmt)
          createProperties(mgmt, models)
          mgmt
            .buildIndex("_label_vertex_index", classOf[Vertex])
            .addKey(mgmt.getPropertyKey("_label"))
            .buildCompositeIndex()

          models.foreach {
            case model: VertexModel =>
              val vertexLabel = mgmt.getVertexLabel(model.label)
              model.indexes.foreach {
                case (indexType, properties) => createIndex(mgmt, classOf[Vertex], vertexLabel, indexType, properties)
              }
            case model: EdgeModel[_, _] =>
              val edgeLabel = mgmt.getEdgeLabel(model.label)
              model.indexes.foreach {
                case (indexType, properties) => createIndex(mgmt, classOf[Edge], edgeLabel, indexType, properties)
              }
          }

          // TODO add index for labels when it will be possible
          // cf. https://github.com/JanusGraph/janusgraph/issues/283
          mgmt.commit()
        }
        Success(())
      }

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(this, graph)
    setSingleProperty(createdVertex, "_createdAt", new Date, createdAtMapping)
    setSingleProperty(createdVertex, "_createdBy", authContext.userId, createdByMapping)
    setSingleProperty(createdVertex, "_label", model.label, UniMapping.string)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.toDomain(createdVertex)(this)
  }

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity = {
    val edgeMaybe = for {
      f <- graph.V(from._id).headOption()
      t <- graph.V(to._id).headOption()
    } yield {
      val createdEdge = model.create(e, f, t)(this, graph)
      setSingleProperty(createdEdge, "_createdAt", new Date, createdAtMapping)
      setSingleProperty(createdEdge, "_createdBy", authContext.userId, createdByMapping)
      setSingleProperty(createdEdge, "_label", model.label, UniMapping.string)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.toDomain(createdEdge)(this)
    }
    edgeMaybe.getOrElse {
      val error = graph.V(from._id).headOption().map(_ => "").getOrElse(s"${from._model.label}:${from._id} not found ") +
        graph.V(to._id).headOption().map(_ => "").getOrElse(s"${to._model.label}:${to._id} not found")
      sys.error(s"Fail to create edge between ${from._model.label}:${from._id} and ${to._model.label}:${to._id}, $error")
    }
  }

  override def labelFilter[E <: Element](model: Model): GremlinScala[E] => GremlinScala[E] = _.has(Key("_label") of model.label)

  override def drop(): Unit = JanusGraphFactory.drop(janusGraph)
}
