package org.thp.scalligraph.janus

import java.lang.{Long => JLong}
import java.nio.file.{Files, Paths}
import java.util.function.Consumer
import java.util.regex.Pattern
import java.util.{Date, Properties}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.config.ConfigObject
import javax.inject.{Inject, Singleton}
import org.apache.tinkerpop.gremlin.process.traversal.{P, Text}
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR
import org.apache.tinkerpop.gremlin.structure._
import org.janusgraph.core.attribute.{Text => JanusText}
import org.janusgraph.core.schema.JanusGraphManagement.IndexJobFuture
import org.janusgraph.core.schema.{Mapping => JanusMapping, _}
import org.janusgraph.core.{Transaction => _, _}
import org.janusgraph.diskstorage.PermanentBackendException
import org.janusgraph.diskstorage.locking.PermanentLockingException
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.olap.job.IndexRepairJob
import org.janusgraph.graphdb.relations.RelationIdentifier
import org.slf4j.MDC
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{MappingCardinality, _}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}
import org.thp.scalligraph.utils.{Config, Retry}
import org.thp.scalligraph.{EntityId, InternalError, NotFoundError}
import play.api.{Configuration, Environment}

import scala.annotation.tailrec
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

  def batchMode: JanusDatabase = {
    val config = janusGraph.configuration()
    config.setProperty("storage.batch-loading", true)
    new JanusDatabase(JanusGraphFactory.open(config), maxAttempts, minBackoff, maxBackoff, randomFactor, chunkSize, system)
  }

  def instanceId: String =
    janusGraph match {
      case g: StandardJanusGraph => g.getConfiguration.getUniqueGraphId
      case _ =>
        logger.error(s"JanusGraph database instance is not a StandardJanusGraph (${janusGraph.getClass}). Instance ID cannot be retrieved.")
        ""
    }
  def openInstances: Set[String] =
    managementTransaction { mgmt =>
      Success {
        mgmt
          .getOpenInstances
          .asScala
          .map {
            case instance if instance.endsWith("(current)") => instance.dropRight(9)
            case instance                                   => instance
          }
          .toSet
      }
    }.get

  def dropConnections(instanceIds: Seq[String]): Unit =
    managementTransaction { mgmt =>
      Success(instanceIds.foreach(mgmt.forceCloseInstance))
    }.get

  def dropOtherConnections: Try[Unit] =
    managementTransaction { mgmt =>
      mgmt.getOpenInstances.asScala.filterNot(_.endsWith("(current)")).foreach(mgmt.forceCloseInstance)
      Success(())
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
    managementTransaction { mgmt =>
      logger.info("Creating database schema")
      createElementLabels(mgmt, models)
      createEntityProperties(mgmt)
      addProperties(mgmt, models)
      Success(())
    }

  override def addSchemaIndexes(models: Seq[Model]): Try[Unit] =
    managementTransaction { mgmt =>
      findFirstAvailableIndex(mgmt, "_label_vertex_index").foreach { indexName =>
        mgmt
          .buildIndex(indexName, classOf[Vertex])
          .addKey(mgmt.getPropertyKey("_label"))
          .buildCompositeIndex()
      }

      models.foreach {
        case model: VertexModel =>
          val vertexLabel = mgmt.getVertexLabel(model.label)
          model.indexes.foreach {
            case (indexType, properties) => createIndex(mgmt, classOf[Vertex], vertexLabel, indexType, properties)
          }
        case model: EdgeModel =>
          val edgeLabel = mgmt.getEdgeLabel(model.label)
          model.indexes.foreach {
            case (indexType, properties) => createIndex(mgmt, classOf[Edge], edgeLabel, indexType, properties)
          }
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
          Option(mgmt.getGraphIndex(indexName)).foreach { index =>
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
      logger.info(s"Reindex job is running: ${scanMetrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT)} record scanned")
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
    }.map(_.foreach(indexName => ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).call()))
      .flatMap { _ =>
        managementTransaction { mgmt => // enable index by reindexing the existing data
          (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
            case index if index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.REGISTERED) =>
              scala.concurrent.blocking {
                showIndexProgress(mgmt.updateIndex(index, SchemaAction.REINDEX))
              }
          }
          Success(())
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

  /**
    * Find the available index name for the specified index base name. If this index is already present and not disable, return None
    * @param mgmt JanusGraph management
    * @param baseIndexName Base index name
    * @return the available index name or None if index is present
    */
  private def findFirstAvailableIndex(mgmt: JanusGraphManagement, baseIndexName: String): Option[String] = {
    val indexNamePattern: Pattern = s"$baseIndexName(?:_\\p{Digit}+)?".r.pattern
    val availableIndexes = (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).filter(i =>
      indexNamePattern.matcher(i.name()).matches()
    )
    if (availableIndexes.isEmpty) Some(baseIndexName)
    else {
      val isEnable = availableIndexes.exists(index => !index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.DISABLED))
      if (isEnable) None
      else {
        val lastIndex = availableIndexes.map(_.name()).toSeq.max
        val version   = lastIndex.drop(baseIndexName.length)
        if (version.isEmpty) Some(s"${baseIndexName}_1")
        else Some(s"${baseIndexName}_${version.tail.toInt + 1}")
      }
    }
  }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexType: IndexType.Value,
      properties: Seq[String]
  ): Unit = {
    val baseIndexName = (elementLabel.name +: properties).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
    findFirstAvailableIndex(mgmt, baseIndexName).foreach { indexName =>
      val index = mgmt.buildIndex(indexName, elementClass).indexOnly(elementLabel)
      val propertyKeys = (properties :+ "_label").map { p =>
        val propertyKey = mgmt.getPropertyKey(p)
        if (propertyKey == null) throw InternalError(s"Property $p in ${elementLabel.name} not found")
        propertyKey
      }
      indexType match {
//        case IndexType.unique =>
//          logger.debug(s"Creating unique index on fields $elementLabel:${propertyKeys.mkString(",")}")
//          propertyKeys.foreach(index.addKey)
//          index.unique()
//          val i = index.buildCompositeIndex()
//          mgmt.setConsistency(i, ConsistencyModifier.LOCK)
//          propertyKeys.foreach { k =>
//            mgmt.setConsistency(k, ConsistencyModifier.LOCK)
//          }
        case IndexType.unique =>
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
      val createdEdge = model.create(e, f, t)(this, graph)
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

  override def labelFilter[D, G <: Element, C <: Converter[D, G]](label: String): Traversal[D, G, C] => Traversal[D, G, C] =
    _.onRaw(_.has("_label", label).hasLabel(label))

  override def mapPredicate[V](predicate: P[V]): P[V] =
    predicate.getBiPredicate match {
      //      case Text.containing    => JanusText.textContains(predicate.getValue) // warning, JanusText.textContains tokenizes the values
      //      case Text.notContaining => JanusText.textContains(predicate.getValue).negate()
      //      case Text.endingWith      => JanusText.textRegex(s"${predicate.getValue}.*")
      //      case Text.notEndingWith   => JanusText.textRegex(s"${predicate.getValue}.*").negate()
      case Text.startingWith    => JanusText.textPrefix(predicate.getValue)
      case Text.notStartingWith => JanusText.textPrefix(predicate.getValue).negate()
      case _                    => predicate
    }

  override def toId(id: Any): JLong = id.toString.toLong

  override def drop(): Unit = JanusGraphFactory.drop(janusGraph)
}
