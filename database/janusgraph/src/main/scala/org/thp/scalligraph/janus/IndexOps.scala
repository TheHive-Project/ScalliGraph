package org.thp.scalligraph.janus

import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Vertex}
import org.janusgraph.core.schema.JanusGraphManagement.IndexJobFuture
import org.janusgraph.core.schema.{JanusGraphManagement, JanusGraphSchemaType, SchemaAction, SchemaStatus, Mapping => JanusMapping}
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.olap.job.IndexRepairJob
import org.thp.scalligraph.models.{EdgeModel, IndexType, Model, VertexModel}
import org.thp.scalligraph.{InternalError, RichSeq}

import java.util.regex.Pattern
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait IndexOps {
  _: JanusDatabase =>
  @tailrec
  private def showIndexProgress(job: IndexJobFuture): Unit =
    if (job.isCancelled)
      logger.warn("Reindex job has been cancelled")
    else if (job.isDone)
      logger.info("Reindex job is finished")
    else {
      val scanMetrics = job.getIntermediateResult
      logger.info(s"Reindex job is running: ${scanMetrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT)} record(s) indexed")
      Thread.sleep(1000)
      showIndexProgress(job)
    }

  def listIndexesWithStatus(status: SchemaStatus): Try[Iterable[String]] =
    managementTransaction { mgmt => // wait for the index to become available
      Try {
        (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).collect {
          case index if index.getFieldKeys.map(index.getIndexStatus).contains(status) => index.name()
        }
      }
    }

  private def waitRegistration(indexName: String): Unit =
    scala.concurrent.blocking {
      logger.info(s"Wait for the index $indexName to become available")
      ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).call()
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
        logger.info(s"Reindex data for $indexName")
        val index = mgmt.getGraphIndex(indexName)
        scala.concurrent.blocking {
          showIndexProgress(mgmt.updateIndex(index, SchemaAction.REINDEX))
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

  override def rebuildIndexes(): Unit = {
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
    *
    * @param mgmt          JanusGraph management
    * @param baseIndexName Base index name
    * @return the available index name or Right if index is present
    */
  private def findFirstAvailableCompositeIndex(mgmt: JanusGraphManagement, baseIndexName: String): Either[String, String] = {
    val indexNamePattern: Pattern = s"$baseIndexName(?:_\\p{Digit}+)?".r.pattern
    val availableIndexes = (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).filter(i =>
      i.isCompositeIndex && indexNamePattern.matcher(i.name()).matches()
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

  /**
    * Find the available index name for the specified index base name. If this index is already present and not disable, return None
    *
    * @param mgmt          JanusGraph management
    * @param baseIndexName Base index name
    * @return the available index name or Right if index is present
    */
  private def findFirstAvailableMixedIndex(mgmt: JanusGraphManagement, baseIndexName: String): Either[String, String] = {
    val validBaseIndexName        = baseIndexName.replaceAll("[^\\p{Alnum}]", baseIndexName)
    val indexNamePattern: Pattern = s"$validBaseIndexName(?:\\p{Digit}+)?".r.pattern
    val availableIndexes = (mgmt.getGraphIndexes(classOf[Vertex]).asScala ++ mgmt.getGraphIndexes(classOf[Edge]).asScala).filter(i =>
      i.isMixedIndex && indexNamePattern.matcher(i.name()).matches()
    )
    if (availableIndexes.isEmpty) Left(baseIndexName)
    else
      availableIndexes
        .find(index => !index.getFieldKeys.map(index.getIndexStatus).contains(SchemaStatus.DISABLED))
        .fold[Either[String, String]] {
          val lastIndex = availableIndexes.map(_.name()).toSeq.max
          val version   = lastIndex.drop(baseIndexName.length)
          if (version.isEmpty) Left(s"${baseIndexName}1")
          else Left(s"$baseIndexName${version.tail.toInt + 1}")
        }(index => Right(index.name()))
  }

  private def createMixedIndex(
      mgmt: JanusGraphManagement,
      elementClass: Class[_ <: Element],
      elementLabel: JanusGraphSchemaType,
      indexDefinitions: Seq[(IndexType.Value, Seq[String])]
  ): Unit = {
    def getPropertyKey(name: String) = Option(mgmt.getPropertyKey(name)).getOrElse(throw InternalError(s"Property $name doesn't exist"))

    findFirstAvailableMixedIndex(mgmt, elementLabel.name) match {
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
                  s"Add index on property ${prop.name()}:${prop.dataType().getSimpleName} (${prop.cardinality()}) to ${elementLabel.name()}"
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
            case (IndexType.fulltextOnly, properties) =>
              properties.foreach { prop =>
                logger.debug(
                  s"Add full-text only index on property ${prop.name()}:${prop.dataType().getSimpleName} (${prop.cardinality()}) to ${elementLabel.name()}"
                )
                mgmt.addIndexKey(index, prop, JanusMapping.TEXT.asParameter())
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

    val (mixedIndexes, compositeIndexes) =
      indexDefinitions.partition(i => i._1 == IndexType.fulltext || i._1 == IndexType.standard || i._1 == IndexType.fulltextOnly)
    if (mixedIndexes.nonEmpty)
      if (fullTextIndexAvailable) createMixedIndex(mgmt, elementClass, elementLabel, mixedIndexes)
      else logger.warn(s"Mixed index is not available.")
    compositeIndexes.foreach {
      case (indexType, properties) =>
        val baseIndexName = (elementLabel.name +: properties).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
        findFirstAvailableCompositeIndex(mgmt, baseIndexName).left.foreach { indexName =>
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

  override def addSchemaIndexes(models: Seq[Model]): Try[Unit] =
    managementTransaction { mgmt =>
      findFirstAvailableCompositeIndex(mgmt, "_label_vertex_index").left.foreach { indexName =>
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

  override def removeIndex(model: String, indexType: IndexType.Value, fields: Seq[String]): Try[Unit] =
    managementTransaction { mgmt =>
      val eitherIndex = indexType match {
        case IndexType.basic | IndexType.unique =>
          val baseIndexName = (model +: fields).map(_.replaceAll("[^\\p{Alnum}]", "").toLowerCase().capitalize).mkString
          findFirstAvailableCompositeIndex(mgmt, baseIndexName)
        case IndexType.standard | IndexType.fulltext | IndexType.fulltextOnly =>
          findFirstAvailableMixedIndex(mgmt, model)
      }
      eitherIndex
        .toOption
        .flatMap(indexName => Option(mgmt.getGraphIndex(indexName)))
        .foreach(index => mgmt.updateIndex(index, SchemaAction.DISABLE_INDEX).get())
      Success(())
    }
}
