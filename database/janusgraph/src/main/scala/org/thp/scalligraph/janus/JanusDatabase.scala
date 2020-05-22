package org.thp.scalligraph.janus

import java.lang.{Long => JLong}
import java.nio.file.{Files, Paths}
import java.util.function.Consumer
import java.util.{Date, Properties}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.config.ConfigObject
import gremlin.scala.{asScalaGraph, Edge, Element, Graph, GremlinScala, Key, P, Vertex}
import javax.inject.{Inject, Singleton}
import org.apache.tinkerpop.gremlin.process.traversal.Text
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR
import org.janusgraph.core.attribute.{Text => JanusText}
import org.janusgraph.core.schema.{Mapping => JanusMapping, _}
import org.janusgraph.core.{Cardinality, JanusGraph, JanusGraphFactory, JanusGraphTransaction, SchemaViolationException}
import org.janusgraph.diskstorage.PermanentBackendException
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.slf4j.MDC
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.utils.{Config, Retry}
import play.api.{Configuration, Environment}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

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

  def close(): Unit = janusGraph.close()

  def isValidId(id: String): Boolean = id.forall(_.isDigit)

  override def roTransaction[R](body: Graph => R): R = {
    val graph             = janusGraph.buildTransaction().readOnly().start()
    val tx                = graph.tx()
    val (oldTx, oldTxMDC) = (localTransaction.get(), Option(MDC.get("tx")))
    try {
      localTransaction.set(Some(tx))
      MDC.put("tx", f"${System.identityHashCode(tx)}%08x")
      logger.debug("Begin of readonly transaction")
      val r = body(graph)
      logger.debug("End of readonly transaction")
      tx.commit()
      r
    } finally {
      if (tx.isOpen) tx.rollback()
      oldTxMDC.fold(MDC.remove("tx"))(MDC.put("tx", _))
      localTransaction.set(oldTx)
    }
  }

  override def source[A](query: Graph => Iterator[A]): Source[A, NotUsed] =
    TransactionHandler[JanusGraphTransaction, A, NotUsed](
      () => janusGraph.buildTransaction().readOnly().start(),
      _.commit(),
      _.rollback(),
      Flow[Graph].flatMapConcat(g => Source.fromIterator(() => query(g)))
    )

  override def source[A, B](body: Graph => (Iterator[A], B)): (Source[A, NotUsed], B) = {
    val tx       = janusGraph.buildTransaction().readOnly().start()
    val (ite, v) = body(tx)
    val src = TransactionHandler[Graph, A, NotUsed](
      () => tx,
      _.tx().commit(),
      _.tx().rollback(),
      Flow[Graph].flatMapConcat(graph => Source.fromIterator(() => ite))
    )
    src -> v
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

  def managementTransaction[R](body: JanusGraphManagement => Try[R]): Try[R] = {
    def commitTransaction(mgmt: JanusGraphManagement): R => R = { r =>
      logger.debug("Committing transaction")
      mgmt.commit()
      logger.debug("End of transaction")
      r
    }

    def rollbackTransaction(mgmt: JanusGraphManagement): PartialFunction[Throwable, Failure[R]] = {
      case t =>
        Try(mgmt.rollback())
        Failure(t)
    }

    Retry(maxAttempts)
      .on[PermanentLockingException]
      .withTry {
        janusGraph.synchronized {
          val mgmt = janusGraph.openManagement()
          Try(body(mgmt))
            .flatten
            .map(commitTransaction(mgmt))
            .recoverWith(rollbackTransaction(mgmt))
        }
      }
  }

  def currentTransactionId(graph: Graph): AnyRef = graph

  override def addTransactionListener(listener: Consumer[Transaction.Status])(implicit graph: Graph): Unit =
    localTransaction.get() match {
      case Some(tx) => tx.addTransactionListener(listener)
      case None     => logger.warn("Trying to add a transaction listener without open transaction")
    }

  override def createSchema(models: Seq[Model]): Try[Unit] =
    Retry(maxAttempts)
      .on[PermanentLockingException]
      .withTry {
        Try {
          logger.info("Creating database schema")
          janusGraph.synchronized {
            val mgmt = janusGraph.openManagement()
            createElementLabels(mgmt, models)
            createEntityProperties(mgmt)
            addProperties(mgmt, models)
            if (mgmt.getGraphIndex("_label_vertex_index") == null)
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
            mgmt.commit()

            // TODO add index for labels when it will be possible
            // cf. https://github.com/JanusGraph/janusgraph/issues/283

//            val mgmt2 = janusGraph.openManagement()
//            //          mgmt2.getGraphIndexes(classOf[Vertex]).asScala.foreach { index =>
//            //            ManagementSystem.awaitGraphIndexStatus(janusGraph, index.name()).call()
//            //          }
//            mgmt2.getGraphIndexes(classOf[Vertex]).asScala.foreach { index =>
//              mgmt2.updateIndex(index, SchemaAction.REINDEX).get()
//            }
//            mgmt2.commit()
          }
        }
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

  private def addProperties(mgmt: JanusGraphManagement, models: Seq[Model]): Unit =
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
        case (propertyName, mapping) =>
          addProperty(mgmt, propertyName, mapping).failed.foreach { error =>
            logger.error(s"Unable to add property $propertyName", error)
          }
      }

  def addProperty[T](model: String, propertyName: String, mapping: Mapping[_, _, _]): Try[Unit] = managementTransaction { mgmt =>
    addProperty(mgmt, propertyName, mapping)
  }

  def addProperty(mgmt: JanusGraphManagement, propertyName: String, mapping: Mapping[_, _, _]): Try[Unit] = {
    logger.debug(s"Create property $propertyName of type ${mapping.graphTypeClass} (${mapping.cardinality})")

    val cardinality = mapping.cardinality match {
      case MappingCardinality.single => Cardinality.SINGLE
      case MappingCardinality.option => Cardinality.SINGLE
      case MappingCardinality.list   => Cardinality.LIST
      case MappingCardinality.set    => Cardinality.SET
    }
    logger.trace(s"mgmt.makePropertyKey($propertyName).dataType(${mapping.graphTypeClass.getSimpleName}.class).cardinality($cardinality).make()")
    Option(mgmt.getPropertyKey(propertyName)) match {
      case None =>
        Try {
          mgmt
            .makePropertyKey(propertyName)
            .dataType(mapping.graphTypeClass)
            .cardinality(cardinality)
            .make()
          ()
        }
      case Some(p) =>
        if (p.dataType() == mapping.graphTypeClass && p.cardinality() == cardinality) {
          logger.info(s"Property $propertyName $cardinality:${mapping.graphTypeClass} already exists, ignore it")
          Success(())
        } else
          Failure(
            InternalError(
              s"Property $propertyName exists with incompatible type: $cardinality:${mapping.graphTypeClass} Vs ${p.cardinality()}:${p.dataType()}"
            )
          )
    }
  }

  override def removeProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean): Try[Unit] =
    if (usedOnlyByThisModel) {
      managementTransaction(mgmt => removeProperty(mgmt, propertyName))
    } else Success(())

  def removeProperty(mgmt: JanusGraphManagement, propertyName: String): Try[Unit] =
    Try {
      Option(mgmt.getPropertyKey(propertyName)).fold(logger.info(s"Cannot remove the property $propertyName, it doesn't exist")) { prop =>
        val newName = s"propertyName-removed-${System.currentTimeMillis()}"
        logger.info(s"Rename the property $propertyName to $newName")
        mgmt.changeName(prop, newName)
      }
    }

  override def addIndex(model: String, indexType: IndexType.Value, properties: Seq[String]): Try[Unit] =
    managementTransaction { mgmt =>
      Option(mgmt.getVertexLabel(model))
        .map(_ -> classOf[Vertex])
        .orElse(Option(mgmt.getEdgeLabel(model)).map(_ -> classOf[Edge]))
        .fold[Try[Unit]](Failure(InternalError(s"Model $model not found"))) {
          case (elementLabel, elementClass) =>
            createIndex(mgmt, elementClass, elementLabel, indexType, properties)
            Success(())
        }
    }
//      .flatMap {
//      case (elementLabel, elementClass) =>
//        managementTransaction { mgmt =>
//          val indexName = (elementLabel.name +: properties).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
//          Option(mgmt.getGraphIndex(indexName)).foreach(mgmt.updateIndex(_, SchemaAction.REINDEX).get())
//          Success(())
//        }
//
//    }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexType: IndexType.Value,
      properties: Seq[String]
  ): Unit = {
    val indexName = (elementLabel.name +: properties).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
    if (mgmt.getGraphIndex(indexName) != null) {
      logger.info(s"Index $indexName already exists, ignore it")
    } else {
      val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
      val propertyKeys = (properties :+ "_label").map { p =>
        val propertyKey = mgmt.getPropertyKey(p)
        if (propertyKey == null) throw InternalError(s"Property $p in ${elementLabel.name} not found")
        propertyKey
      }
      indexType match {
        case IndexType.unique =>
          logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.unique()
          val i = index.buildCompositeIndex()
          mgmt.setConsistency(i, ConsistencyModifier.LOCK)
          propertyKeys.foreach { k =>
            mgmt.setConsistency(k, ConsistencyModifier.LOCK)
          }
        case IndexType.tryUnique =>
          logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.unique()
          index.buildCompositeIndex()
        case IndexType.basic =>
          logger.debug(s"Creating basic index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.buildCompositeIndex()
        case IndexType.standard =>
          logger.debug(s"Creating index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(index.addKey)
          index.buildMixedIndex("search")
        case IndexType.fulltext =>
          logger.debug(s"Creating fulltext index on fields $elementLabel:${propertyKeys.mkString(",")}")
          propertyKeys.foreach(k => index.addKey(k, JanusMapping.TEXT.asParameter()))
          index.buildMixedIndex("search")
      }
      ()
    }
  }

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(this, graph)
    val entity        = DummyEntity(model, createdVertex.id(), authContext.userId)
    setSingleProperty(createdVertex, "_createdAt", entity._createdAt, createdAtMapping)
    setSingleProperty(createdVertex, "_createdBy", entity._createdBy, createdByMapping)
    setSingleProperty(createdVertex, "_label", model.label, UniMapping.string)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.addEntity(v, entity)
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
      val entity      = DummyEntity(model, createdEdge.id(), authContext.userId)
      setSingleProperty(createdEdge, "_createdAt", entity._createdAt, createdAtMapping)
      setSingleProperty(createdEdge, "_createdBy", entity._createdBy, createdByMapping)
      setSingleProperty(createdEdge, "_label", model.label, UniMapping.string)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.addEntity(e, entity)
    }
    edgeMaybe.getOrElse {
      val error = graph.V(from._id).headOption().map(_ => "").getOrElse(s"${from._model.label}:${from._id} not found ") +
        graph.V(to._id).headOption().map(_ => "").getOrElse(s"${to._model.label}:${to._id} not found")
      sys.error(s"Fail to create edge between ${from._model.label}:${from._id} and ${to._model.label}:${to._id}, $error")
    }
  }

  override def labelFilter[E <: Element](label: String): GremlinScala[E] => GremlinScala[E] = _.has(Key("_label") of label)

  override def mapPredicate[T](predicate: P[T]): P[T] =
    predicate.getBiPredicate match {
      case Text.containing    => JanusText.textContains(predicate.getValue)
      case Text.notContaining => JanusText.textContains(predicate.getValue).negate()
      //      case Text.endingWith      => JanusText.textRegex(s"${predicate.getValue}.*")
      //      case Text.notEndingWith   => JanusText.textRegex(s"${predicate.getValue}.*").negate()
      case Text.startingWith    => JanusText.textPrefix(predicate.getValue)
      case Text.notStartingWith => JanusText.textPrefix(predicate.getValue).negate()
      case _                    => predicate
    }

  override def toId(id: Any): JLong = id.toString.toLong

  override def drop(): Unit = JanusGraphFactory.drop(janusGraph)
}
