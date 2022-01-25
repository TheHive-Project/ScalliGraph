package org.thp.scalligraph.janus

import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Vertex}
import org.janusgraph.core.schema.JanusGraphManagement.IndexJobFuture
import org.janusgraph.core.schema.{JanusGraphManagement, Parameter, SchemaAction, SchemaStatus, Mapping => JanusMapping}
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.types.ParameterType
import org.thp.scalligraph.models.{IndexType, Model, VertexModel}
import org.thp.scalligraph.{InternalError, RichSeq}

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

trait IndexOps {
  _: JanusDatabase =>
  @tailrec
  private def showIndexProgress(jobId: String, job: IndexJobFuture): Unit =
    if (job.isCancelled)
      logger.warn(s"Reindex job $jobId has been cancelled")
    else if (job.isDone)
      logger.info(s"Reindex job $jobId is finished")
    else {
      logger.info(s"Reindex job $jobId is running")
      Thread.sleep(1000)
      showIndexProgress(jobId, job)
    }

  def listIndexesWithStatus(statuses: SchemaStatus*): Try[Iterable[String]] =
    managementTransaction { mgmt =>
      Try {
        (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
          case index if index.getFieldKeys.map(index.getIndexStatus).exists(statuses.contains) => index.name()
        }
      }
    }

  def fieldIsIndexed(fieldName: String): Boolean = {
    val indices = listIndexesWithStatus(SchemaStatus.ENABLED).getOrElse(Nil)
    managementTransaction { mgmt =>
      Try(indices.exists(i => mgmt.getGraphIndex(i).getFieldKeys.exists(_.name() == fieldName)))
    }
      .getOrElse(false)
  }

  private def waitRegistration(indexName: String): Unit =
    scala.concurrent.blocking {
      logger.info(s"Wait for the index $indexName to become available")
      ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call()
      ()
    }

  private def forceRegistration(indexName: String): Try[Unit] =
    managementTransaction { mgmt =>
      Try {
        logger.info(s"Force registration of index $indexName")
        val index = mgmt.getGraphIndex(indexName)
        scala.concurrent.blocking {
          mgmt.updateIndex(index, SchemaAction.REGISTER_INDEX).get()
        }
        ()
      }
    }

  private def reindex(indexName: String): Try[Unit] =
    managementTransaction { mgmt => // enable index by reindexing the existing data
      Try {
        val index = mgmt.getGraphIndex(indexName)
        scala.concurrent.blocking {
          val job   = mgmt.updateIndex(index, SchemaAction.REINDEX)
          val jobId = f"${System.identityHashCode(job)}%08x"
          logger.info(s"Reindex data for $indexName (job: $jobId)")
          showIndexProgress(jobId, job)
        }
      }
    }

  override def reindexData(model: String): Try[Unit] = {
    logger.info(s"Reindex data of model $model")
    for {
      indexList <- listIndexesWithStatus(SchemaStatus.ENABLED)
      _         <- indexList.filter(_.startsWith(model)).toTry(reindex)
    } yield ()
  }

  override def reindexData(): Unit = {
    listIndexesWithStatus(SchemaStatus.ENABLED)
      .foreach(_.foreach(reindex))
    ()
  }

  private def enableIndexes(): Try[Unit] =
    for {
      installedIndexes <- listIndexesWithStatus(SchemaStatus.INSTALLED)
      _ = installedIndexes.foreach(waitRegistration)
      blockedIndexes <- listIndexesWithStatus(SchemaStatus.INSTALLED)
      _ = blockedIndexes.foreach(forceRegistration)
      registeredIndexes <- listIndexesWithStatus(SchemaStatus.REGISTERED)
      _ = registeredIndexes.foreach(reindex)
    } yield ()

  override def addIndex(model: String, indexDefinition: Seq[(IndexType, Seq[String])]): Try[Unit] =
    managementTransaction { mgmt =>
      Option(mgmt.getVertexLabel(model))
        .map(_ => classOf[Vertex])
        .orElse(Option(mgmt.getEdgeLabel(model)).map(_ => classOf[Edge]))
        .fold[Try[Unit]](Failure(InternalError(s"Model $model not found"))) { elementClass =>
          createIndex(mgmt, elementClass, indexDefinition.map(i => (model, i._1, i._2)))
          Success(())
        }
    }

  /**
    * Find the available index name for the specified index base name. If this index is already present and not disable, return None
    *
    * @param mgmt          JanusGraph management
    * @param baseIndexName Base index name
    * @return the available index name or Right if index is present
    */
  protected def findFirstAvailableIndex(mgmt: JanusGraphManagement, baseIndexName: String): Either[String, String] = {
    val indexNamePattern: Pattern = s"$baseIndexName(?:_?\\p{Digit}+)?".r.pattern
    val availableIndexes = (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala)
      .filter(i => indexNamePattern.matcher(i.name()).matches())
    if (availableIndexes.isEmpty) Left(baseIndexName)
    else
      availableIndexes
        .find(index => !index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.DISABLED))
        .fold[Either[String, String]] {
          val lastIndex = availableIndexes
            .map(_.name().drop(baseIndexName.length).dropWhile(!_.isDigit))
            .map {
              case "" => 0
              case n  => n.toInt
            }
            .max
          Left(s"$baseIndexName${lastIndex + 1}")
        }(index => Right(index.name()))
  }

  private def createIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      indexDefinitions: Seq[(String, IndexType, Seq[String])]
  ): Boolean = {
    def getPropertyKey(name: String) = Option(mgmt.getPropertyKey(name)).getOrElse(throw InternalError(s"Property $name doesn't exist"))

    // check if a property hasn't different index types
    val groupedIndex: Map[String, Seq[IndexType]] = indexDefinitions
      .flatMap {
        case (_, IndexType.fulltextOnly, props) => props.filterNot(_.startsWith("_")).map(_ -> IndexType.fulltextOnly)
        case (_, _, props)                      => props.filterNot(_.startsWith("_")).map(_ -> IndexType.standard)
      }
      .groupBy(_._1)
      .view
      .filterKeys(!_.startsWith("_"))
      .mapValues(_.map(_._2).distinct)
      .toMap ++ Seq(
      "_label"     -> Seq(IndexType.standard),
      "_createdAt" -> Seq(IndexType.standard),
      "_createdBy" -> Seq(IndexType.standard),
      "_updatedAt" -> Seq(IndexType.standard),
      "_updatedBy" -> Seq(IndexType.standard)
    )

    findFirstAvailableIndex(mgmt, "global") match {
      case Left(indexName) =>
        logger.debug(s"Creating index on fields $indexName")

        val index = mgmt.buildIndex(indexName, elementClass)
        groupedIndex.foreach {
          case (p, Seq(IndexType.fulltextOnly)) =>
            index.addKey(getPropertyKey(p), JanusMapping.TEXT.asParameter(), Parameter.of(ParameterType.customParameterName("fielddata"), true))
          case (p, _) =>
            val prop = getPropertyKey(p)
            if (prop.dataType() == classOf[String])
              index.addKey(prop, JanusMapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.customParameterName("fielddata"), true))
            else index.addKey(prop, JanusMapping.DEFAULT.asParameter())
        }
        index.buildMixedIndex("search")
        true
      case Right(indexName) =>
        val index           = mgmt.getGraphIndex(indexName)
        val indexProperties = index.getFieldKeys.map(_.name())

        groupedIndex
          .flatMap {
            case (prop, indexType) if !indexProperties.contains(prop) => Option(getPropertyKey(prop)).map(_ -> indexType)
            case _                                                    => None
          }
          .map {
            case (p, Seq(IndexType.fulltextOnly)) =>
              logger.debug(s"Add full-text only index on property ${p.name()}:${p.dataType().getSimpleName} (${p.cardinality()})")
              mgmt.addIndexKey(index, p, JanusMapping.TEXT.asParameter(), Parameter.of(ParameterType.customParameterName("fielddata"), true))
            case (p, _) =>
              logger.debug(s"Add index on property ${p.name()}:${p.dataType().getSimpleName} (${p.cardinality()})")
              if (p.dataType() == classOf[String])
                mgmt.addIndexKey(index, p, JanusMapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.customParameterName("fielddata"), true))
              else mgmt.addIndexKey(index, p, JanusMapping.DEFAULT.asParameter())
          }
          .nonEmpty
    }
  }

  override def addSchemaIndexes(models: Seq[Model]): Try[Boolean] =
    managementTransaction { mgmt =>
      val (vertexModels, edgeModels) = models.partition(_.isInstanceOf[VertexModel])
      val vertexIndexUpdated         = createIndex(mgmt, classOf[Vertex], vertexModels.flatMap(m => m.indexes.map(i => (m.label, i._1, i._2))))
      val edgeIndexUpdated           = createIndex(mgmt, classOf[Edge], edgeModels.flatMap(m => m.indexes.map(i => (m.label, i._1, i._2))))
      Success(vertexIndexUpdated || edgeIndexUpdated)
    }.flatMap(indexUpdated => enableIndexes().map(_ => indexUpdated))

  private def dropIndex(indexName: String): Unit = {
    managementTransaction { mgmt =>
      Option(mgmt.getGraphIndex(indexName))
        .fold[Try[Unit]](Success(())) { index =>
          Try {
            mgmt.updateIndex(index, SchemaAction.REMOVE_INDEX).get
            ()
          }
        }
        .recover {
          case error =>
            logger.warn(s"The index $indexName cannot be removed ($error)")
        }
    }
    ()
  }

  override def removeIndex(model: String, indexType: IndexType, fields: Seq[String]): Try[Unit] =
    managementTransaction { mgmt =>
      val indexPrefix = model.toLowerCase.capitalize
      listIndexesWithStatus(SchemaStatus.ENABLED, SchemaStatus.INSTALLED, SchemaStatus.REGISTERED).map { indexes =>
        indexes
          .filter(_.startsWith(indexPrefix))
          .flatMap(indexName => Option(mgmt.getGraphIndex(indexName)))
          .map { index =>
            logger.info(s"Disable index ${index.name()}")
            scala.concurrent.blocking(mgmt.updateIndex(index, SchemaAction.DISABLE_INDEX).get())
            index.name()
          }
      }
    }
      .map { indexes =>
        indexes.foreach { indexName =>
          scala.concurrent.blocking {
            logger.info(s"Wait for the index $indexName to become disabled")
            ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.DISABLED).call()
            dropIndex(indexName)
          }
        }
      }

  override def removeAllIndex(): Try[Unit] =
    managementTransaction { mgmt =>
      listIndexesWithStatus(SchemaStatus.ENABLED, SchemaStatus.INSTALLED, SchemaStatus.REGISTERED).map { indexes =>
        indexes
          .flatMap(indexName => Option(mgmt.getGraphIndex(indexName)))
          .map { index =>
            logger.info(s"Disable index ${index.name()}")
            scala.concurrent.blocking(mgmt.updateIndex(index, SchemaAction.DISABLE_INDEX).get())
            index.name()
          }
      }
    }
      .map { indexes =>
        indexes.foreach { indexName =>
          scala.concurrent.blocking {
            logger.info(s"Wait for the index $indexName to become disabled")
            ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.DISABLED).call()
          }
        }
        // try to remove all disabled indices
        listIndexesWithStatus(SchemaStatus.DISABLED).foreach(_.foreach(dropIndex))
      }
}
