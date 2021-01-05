package org.thp.scalligraph.janus

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.config.ConfigObject
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Transaction, Vertex, Graph => TinkerGraph}
import org.janusgraph.core.schema.JanusGraphManagement.IndexJobFuture
import org.janusgraph.core.schema.{Mapping => JanusMapping, _}
import org.janusgraph.core.{Transaction => _, _}
import org.janusgraph.diskstorage.PermanentBackendException
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.olap.job.IndexRepairJob
import org.janusgraph.graphdb.relations.RelationIdentifier
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStepStrategy
import org.slf4j.MDC
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{MappingCardinality, _}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Graph, GraphWrapper, Traversal}
import org.thp.scalligraph.utils.{Config, Retry}
import org.thp.scalligraph.{EntityId, InternalError, NotFoundError, SingleInstance}
import play.api.{Configuration, Logger}
import org.thp.scalligraph.janus.strategies._

import java.lang.{Long => JLong}
import java.nio.file.{Files, Paths}
import java.util.function.Consumer
import java.util.regex.Pattern
import java.util.{Date, Properties}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object JanusDatabase {
  lazy val logger: Logger = Logger(getClass)

  def openDatabase(configuration: Configuration, system: ActorSystem): JanusGraph = {
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
    val maxAttempts  = configuration.get[Int]("db.janusgraph.connect.maxAttempts")
    val minBackoff   = configuration.get[FiniteDuration]("db.janusgraph.connect.minBackoff")
    val maxBackoff   = configuration.get[FiniteDuration]("db.janusgraph.connect.maxBackoff")
    val randomFactor = configuration.get[Double]("db.janusgraph.connect.randomFactor")

    Retry(maxAttempts)
      .on[IllegalArgumentException]
      .withBackoff(minBackoff, maxBackoff, randomFactor)(system.scheduler, system.dispatcher)
      .sync {
        JanusGraphFactory.open(new Config(configuration.get[Configuration]("db.janusgraph")))
      }
  }

  def fullTextAvailable(db: JanusGraph, singleInstance: SingleInstance): Boolean = {
    val mgmt = db.openManagement()
    try {
      lazy val location = mgmt.get("index.search.directory")
      mgmt.get("index.search.backend") match {
        case "elasticsearch" =>
          logger.info(s"Full-text index is available (elasticsearch:${mgmt.get("index.search.hostname")}) $singleInstance")
          true
        case "lucene" if singleInstance.value && location.startsWith("/") && !location.startsWith("/tmp") =>
          logger.info(s"Full-text index is available (lucene:$location) $singleInstance")
          true
        case "lucene" =>
          val reason =
            if (!singleInstance.value) "lucene index can't be used in cluster mode"
            else if (!location.startsWith("/")) "index path must be absolute"
            else if (location.startsWith("/tmp")) "index path must not in /tmp"
            else "no reason ?!"
          logger.warn(s"Full-text index is NOT available (lucene:$location) $singleInstance: $reason")
          false
      }
    } finally mgmt.commit()
  }
}

class JanusDatabase(
    janusGraph: JanusGraph,
    maxAttempts: Int,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    override val chunkSize: Int,
    system: ActorSystem,
    override val fullTextIndexAvailable: Boolean
) extends BaseDatabase {
  val name = "janus"

  val localTransaction: ThreadLocal[Option[Transaction]] = ThreadLocal.withInitial[Option[Transaction]](() => None)
  janusGraph.tx.onReadWrite(READ_WRITE_BEHAVIOR.MANUAL)

  def this(
      janusGraph: JanusGraph,
      configuration: Configuration,
      system: ActorSystem,
      fullTextAvailable: Boolean
  ) =
    this(
      janusGraph,
      configuration.get[Int]("db.onConflict.maxAttempts"),
      configuration.get[FiniteDuration]("db.onConflict.minBackoff"),
      configuration.get[FiniteDuration]("db.onConflict.maxBackoff"),
      configuration.get[Double]("db.onConflict.randomFactor"),
      configuration.underlying.getBytes("db.chunkSize").toInt,
      system,
      fullTextAvailable
    )

  def this(
      janusGraph: JanusGraph,
      configuration: Configuration,
      system: ActorSystem,
      singleInstance: SingleInstance
  ) =
    this(
      janusGraph,
      configuration,
      system,
      JanusDatabase.fullTextAvailable(janusGraph, singleInstance)
    )

  def this(configuration: Configuration, system: ActorSystem, singleInstance: SingleInstance) =
    this(JanusDatabase.openDatabase(configuration, system), configuration, system, singleInstance)

  def this(configuration: Configuration, system: ActorSystem, fullTextIndexAvailable: Boolean) =
    this(JanusDatabase.openDatabase(configuration, system), configuration, system, fullTextIndexAvailable)

  def batchMode: JanusDatabase = {
    val config = janusGraph.configuration()
    config.setProperty("storage.batch-loading", true)
    new JanusDatabase(
      JanusGraphFactory.open(config),
      maxAttempts,
      minBackoff,
      maxBackoff,
      randomFactor,
      chunkSize,
      system,
      fullTextIndexAvailable
    )
  }

  override def close(): Unit = janusGraph.close()

  private def parseId(s: String): AnyRef = Try(JLong.valueOf(s)).getOrElse(RelationIdentifier.parse(s))
  override val idMapping: Mapping[EntityId, EntityId, AnyRef] = new Mapping[EntityId, EntityId, AnyRef](id => parseId(id.value), EntityId.apply) {
    override val cardinality: MappingCardinality.Value                = MappingCardinality.single
    override def getProperty(element: Element, key: String): EntityId = throw InternalError(s"ID mapping can't be used for attribute $key")
    override def setProperty(element: Element, key: String, value: EntityId): Unit =
      throw InternalError(s"ID mapping can't be used for attribute $key")
    override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
        traversal: Traversal[TD, TG, TC],
        key: String,
        value: EntityId
    ): Traversal[TD, TG, TC] =
      throw InternalError(s"ID mapping can't be used for attribute $key")
    override def wrap(us: Seq[EntityId]): EntityId = us.head
  }

  override def roTransaction[R](body: Graph => R): R = {
    val graph             = new GraphWrapper(this, janusGraph.buildTransaction().readOnly().start())
    val tx                = graph.tx()
    val (oldTx, oldTxMDC) = (localTransaction.get(), Option(MDC.get("tx")))
    try {
      localTransaction.set(Some(tx))
      MDC.put("tx", f"${System.identityHashCode(tx)}%08x")
      if (logger.isDebugEnabled) {
        logger.debug("Begin of readonly transaction")
        val start = System.currentTimeMillis()
        val r     = body(graph)
        logger.debug(s"End of readonly transaction (${System.currentTimeMillis() - start}ms)")
        tx.commit()
        r
      } else {
        val r = body(graph)
        tx.commit()
        r
      }
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
      Flow[TinkerGraph].flatMapConcat(g => Source.fromIterator(() => query(new GraphWrapper(this, g))))
    )

  override def source[A, B](body: Graph => (Iterator[A], B)): (Source[A, NotUsed], B) = {
    val tx       = new GraphWrapper(this, janusGraph.buildTransaction().readOnly().start())
    val (ite, v) = body(tx)
    val src = TransactionHandler[Graph, A, NotUsed](
      () => tx,
      _.tx().commit(),
      _.tx().rollback(),
      Flow[Graph].flatMapConcat(_ => Source.fromIterator(() => ite))
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
        .withBackoff(minBackoff, maxBackoff, randomFactor)(system.scheduler, system.dispatcher)
        .withTry {
          val graph = new GraphWrapper(this, janusGraph.buildTransaction().start())
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
    managementTransaction { mgmt =>
      logger.info("Creating database schema")
      createElementLabels(mgmt, models)
      createEntityProperties(mgmt)
      addProperties(mgmt, models)
      Success(())
    }

  override def addVertexModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit] =
    managementTransaction { mgmt =>
      mgmt.getOrCreateVertexLabel(label)
      properties.toTry {
        case (property, mapping) => addProperty(mgmt, property, mapping)
      }
    }.map(_ => ())

  override def addEdgeModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit] =
    managementTransaction { mgmt =>
      mgmt.getOrCreateEdgeLabel(label)
      properties.toTry {
        case (property, mapping) => addProperty(mgmt, property, mapping)
      }
    }.map(_ => ())

  override def addSchemaIndexes(models: Seq[Model]): Try[Unit] =
    managementTransaction { mgmt =>
      findFirstAvailableIndex(mgmt, "_label_vertex_index").left.foreach { indexName =>
        mgmt
          .buildIndex(indexName, classOf[Vertex])
          .addKey(mgmt.getPropertyKey("_label"))
          .buildCompositeIndex()
      }

      models.foreach {
        case model: VertexModel =>
          val vertexLabel = mgmt.getVertexLabel(model.label)
          createIndex(mgmt, classOf[Vertex], vertexLabel, model.indexes)
        case model: EdgeModel =>
          val edgeLabel = mgmt.getEdgeLabel(model.label)
          createIndex(mgmt, classOf[Edge], edgeLabel, model.indexes)
        // TODO add index for labels when it will be possible
        // cf. https://github.com/JanusGraph/janusgraph/issues/283
      }
      Success(())
    }.flatMap(_ => enableIndexes())

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
      case m: EdgeModel if Option(mgmt.getEdgeLabel(m.label)).isEmpty =>
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

  def addProperty(model: String, propertyName: String, mapping: Mapping[_, _, _]): Try[Unit] =
    managementTransaction { mgmt =>
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
    if (usedOnlyByThisModel)
      managementTransaction(mgmt => removeProperty(mgmt, propertyName))
    else Success(())

  def removeProperty(mgmt: JanusGraphManagement, propertyName: String): Try[Unit] =
    Try {
      Option(mgmt.getPropertyKey(propertyName)).fold(logger.info(s"Cannot remove the property $propertyName, it doesn't exist")) { prop =>
        val newName = s"propertyName-removed-${System.currentTimeMillis()}"
        logger.info(s"Rename the property $propertyName to $newName")
        mgmt.changeName(prop, newName)
      }
    }

  override def removeAllIndexes(): Unit =
    managementTransaction { mgmt =>
      Success((mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).map(_.name()))
    }.foreach(_.foreach(removeIndex))

  def removeIndexes(baseIndexName: String): Unit =
    managementTransaction { mgmt =>
      val indexNamePattern: Pattern = s"$baseIndexName(?:_\\p{Digit}+)?".r.pattern
      Success((mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
        case index if indexNamePattern.matcher(index.name()).matches() => index.name()
      })
    }.foreach(_.foreach(removeIndex))

  private def removeIndex(indexName: String) =
    managementTransaction { mgmt =>
      Option(mgmt.getGraphIndex(indexName)).fold[Try[Unit]](Failure(NotFoundError(s"Index $indexName doesn't exist"))) { index =>
        if (!index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.DISABLED)) {
          logger.debug(s"Disable index $indexName")
          mgmt.updateIndex(index, SchemaAction.DISABLE_INDEX)
        } else
          logger.debug(s"Index $indexName is already disable")
        Success(())
      }
    }.map(_ => ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.DISABLED).call())
      .flatMap { _ =>
        managementTransaction { mgmt =>
          Option(mgmt.getGraphIndex(indexName)).filter(_.isCompositeIndex).foreach { index =>
            mgmt.updateIndex(index, SchemaAction.REMOVE_INDEX)
          }
          Success(())
        }
      }

  @tailrec
  private def showIndexProgress(job: IndexJobFuture): Unit =
    if (job.isCancelled)
      logger.warn("Reindex job has been cancelled")
    else if (job.isDone)
      logger.info("Reindex job is finished")
    else {
      val scanMetrics = job.getIntermediateResult
      logger.info(s"Reindex job is running: ${scanMetrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT)} record indexed")
      Thread.sleep(1000)
      showIndexProgress(job)
    }

  override def enableIndexes(): Try[Unit] =
    managementTransaction { mgmt => // wait for the index to become available
      Try {
        (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
          case index if index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.INSTALLED) => index.name()
        }
      }
    }.map(_.foreach { indexName =>
      logger.info(s"Wait for the index $indexName to become available")
      ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).call()
    }).flatMap { _ =>
      managementTransaction { mgmt => // enable index by reindexing the existing data
        (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
          case index if index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.REGISTERED) =>
            logger.info(s"Reindex data for ${index.name()}")
            scala.concurrent.blocking {
              showIndexProgress(mgmt.updateIndex(index, SchemaAction.REINDEX))
            }
        }
        Success(())
      }
    }

  override def addIndex(model: String, indexDefinition: Seq[(IndexType.Value, Seq[String])]): Try[Unit] =
    managementTransaction { mgmt =>
      Option(mgmt.getVertexLabel(model))
        .map(_ -> classOf[Vertex])
        .orElse(Option(mgmt.getEdgeLabel(model)).map(_ -> classOf[Edge]))
        .fold[Try[Unit]](Failure(InternalError(s"Model $model not found"))) {
          case (elementLabel, elementClass) =>
            createIndex(mgmt, elementClass, elementLabel, indexDefinition)
            Success(())
        }
    }

  /**
    * Find the available index name for the specified index base name. If this index is already present and not disable, return None
    * @param mgmt JanusGraph management
    * @param baseIndexName Base index name
    * @return the available index name or Right if index is present
    */
  private def findFirstAvailableIndex(mgmt: JanusGraphManagement, baseIndexName: String): Either[String, String] = {
    val indexNamePattern: Pattern = s"$baseIndexName(?:_\\p{Digit}+)?".r.pattern
    val availableIndexes = (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).filter(i =>
      indexNamePattern.matcher(i.name()).matches()
    )
    if (availableIndexes.isEmpty) Left(baseIndexName)
    else
      availableIndexes
        .find(index => !index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.DISABLED))
        .fold[Either[String, String]] {
          val lastIndex = availableIndexes.map(_.name()).toSeq.max
          val version   = lastIndex.drop(baseIndexName.length)
          if (version.isEmpty) Left(s"${baseIndexName}_1")
          else Left(s"${baseIndexName}_${version.tail.toInt + 1}")
        }(index => Right(index.name()))
  }

  def createMixedIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexDefinitions: Seq[(IndexType.Value, Seq[String])]
  ): Unit = {
    def getPropertyKey(name: String) = Option(mgmt.getPropertyKey(name)).getOrElse(throw InternalError(s"Property $name doesnt exist"))

    val indexBaseName = s"${elementLabel.name}_mixed"
    findFirstAvailableIndex(mgmt, indexBaseName) match {
      case Left(indexName) =>
        logger.debug(s"Creating index on fields $indexName")
        val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
        index.addKey(getPropertyKey("_label"), JanusMapping.STRING.asParameter())
        indexDefinitions.foreach {
          case (IndexType.standard, properties) =>
            properties.foreach { propName =>
              val prop = getPropertyKey(propName)
              if (prop.dataType() == classOf[String]) index.addKey(prop, JanusMapping.STRING.asParameter())
              else index.addKey(prop, JanusMapping.DEFAULT.asParameter())
            }
          case (IndexType.fulltext, properties) => properties.foreach(p => index.addKey(getPropertyKey(p), JanusMapping.TEXTSTRING.asParameter()))
          case _                                => // Not possible
        }
        index.buildMixedIndex("search")
        ()
      case Right(indexName) =>
        val index           = mgmt.getGraphIndex(indexName)
        val indexProperties = index.getFieldKeys.map(_.name())
        val newProperties = indexDefinitions.map {
          case (tpe, properties) => tpe -> properties.filterNot(indexProperties.contains).map(getPropertyKey)
        }
        newProperties
          .foreach {
            case (IndexType.standard, properties) =>
              properties.foreach { prop =>
                logger.debug(
                  s"Add index pn property ${prop.name()}:${prop.dataType().getSimpleName} (${prop.cardinality()}) to ${elementLabel.name()}"
                )
                if (prop.dataType() == classOf[String]) mgmt.addIndexKey(index, prop, JanusMapping.STRING.asParameter())
                else mgmt.addIndexKey(index, prop, JanusMapping.DEFAULT.asParameter())
              }
            case (IndexType.fulltext, properties) =>
              properties.foreach { prop =>
                logger.debug(
                  s"Add full-text index on property ${prop.name()}:${prop.dataType().getSimpleName} (${prop.cardinality()}) to ${elementLabel.name()}"
                )
                mgmt.addIndexKey(index, prop, JanusMapping.TEXTSTRING.asParameter())
              }
            case _ => // Not possible
          }
    }
  }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexDefinitions: Seq[(IndexType.Value, Seq[String])]
  ): Unit = {
    def getPropertyKey(name: String) = Option(mgmt.getPropertyKey(name)).getOrElse(throw InternalError(s"Property $name doesnt exist"))

    val labelProperty = getPropertyKey("_label")

    val (mixedIndexes, compositeIndexes) = indexDefinitions.partition(i => i._1 == IndexType.fulltext || i._1 == IndexType.standard)
    if (mixedIndexes.nonEmpty)
      if (fullTextIndexAvailable) createMixedIndex(mgmt, elementClass, elementLabel, mixedIndexes)
      else logger.warn(s"Mixed index is not available.")
    compositeIndexes.foreach {
      case (indexType, properties) =>
        val baseIndexName = (elementLabel.name +: properties).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
        findFirstAvailableIndex(mgmt, baseIndexName).left.foreach { indexName =>
          val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
          index.addKey(labelProperty)
          properties.foreach(p => index.addKey(getPropertyKey(p)))
          if (indexType == IndexType.unique) {
            logger.debug(s"Creating unique index on fields $elementLabel:${properties.mkString(",")}")
            index.unique()
          } else logger.debug(s"Creating basic index on fields $elementLabel:${properties.mkString(",")}")
          index.buildCompositeIndex()
        }
    }
  }

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(graph)
    val entity        = DummyEntity(model.label, createdVertex.id(), authContext.userId)
    createdAtMapping.setProperty(createdVertex, "_createdAt", entity._createdAt)
    createdByMapping.setProperty(createdVertex, "_createdBy", entity._createdBy)
    UMapping.string.setProperty(createdVertex, "_label", model.label)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.addEntity(v, entity)
  }

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity = {
    val edgeMaybe = for {
      f <- Traversal.V(from._id)(graph).headOption
      t <- Traversal.V(to._id)(graph).headOption
    } yield {
      val createdEdge = model.create(e, f, t)(graph)
      val entity      = DummyEntity(model.label, createdEdge.id(), authContext.userId)
      createdAtMapping.setProperty(createdEdge, "_createdAt", entity._createdAt)
      createdByMapping.setProperty(createdEdge, "_createdBy", entity._createdBy)
      UMapping.string.setProperty(createdEdge, "_label", model.label)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.addEntity(e, entity)
    }
    edgeMaybe.getOrElse {
      val error = Traversal.V(from._id)(graph).headOption.map(_ => "").getOrElse(s"${from._label}:${from._id} not found ") +
        Traversal.V(to._id)(graph).headOption.map(_ => "").getOrElse(s"${to._label}:${to._id} not found")
      sys.error(s"Fail to create edge between ${from._label}:${from._id} and ${to._label}:${to._id}, $error")
    }
  }

  override def labelFilter[D, G, C <: Converter[D, G]](label: String, traversal: Traversal[D, G, C]): Traversal[D, G, C] =
    traversal.onRaw(_.hasLabel(label).has("_label", label))
//  override def labelFilter[D, G <: Element, C <: Converter[D, G]](label: String): Traversal[D, G, C] => Traversal[D, G, C] =
//    _.onRaw(_.has("_label", label).hasLabel(label))

  def V[D <: Product](ids: EntityId*)(implicit model: Model.Vertex[D], graph: Graph): Traversal.V[D] =
    new Traversal[D with Entity, Vertex, Converter[D with Entity, Vertex]](
      graph,
      graph
        .traversal()
        .asInstanceOf[TraversalSource]
        .withoutStrategies(classOf[JanusGraphStepStrategy])
        .withStrategies(IndexOptimizerStrategy.instance(), JanusGraphAcceptNullStrategy.instance())
        .asInstanceOf[GraphTraversalSource]
        .V(ids.map(_.value): _*)
        .hasLabel(model.label)
        .has("_label", model.label),
      model.converter
    )
  def E[D <: Product](ids: EntityId*)(implicit model: Model.Edge[D], graph: Graph): Traversal.E[D] =
    new Traversal[D with Entity, Edge, Converter[D with Entity, Edge]](
      graph,
      graph.traversal().E(ids.map(_.value): _*).hasLabel(model.label).has("_label", model.label),
      model.converter
    )

  def V(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Vertex, Vertex, Converter.Identity[Vertex]] =
    new Traversal[Vertex, Vertex, Converter.Identity[Vertex]](
      graph,
      graph
        .traversal()
        .asInstanceOf[TraversalSource]
        .withoutStrategies(classOf[JanusGraphStepStrategy])
        .withStrategies(IndexOptimizerStrategy.instance(), JanusGraphAcceptNullStrategy.instance())
        .asInstanceOf[GraphTraversalSource]
        .V(ids.map(_.value): _*)
        .hasLabel(label)
        .has("_label", label),
      Converter.identity[Vertex]
    )
  def E(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Edge, Edge, Converter.Identity[Edge]] =
    new Traversal[Edge, Edge, Converter.Identity[Edge]](
      graph,
      graph.traversal().E(ids.map(_.value): _*).hasLabel(label).has("_label", label),
      Converter.identity[Edge]
    )

  override def toId(id: Any): JLong = id.toString.toLong

  override def drop(): Unit = JanusGraphFactory.drop(janusGraph)
}
