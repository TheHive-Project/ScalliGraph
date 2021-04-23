package org.thp.scalligraph.janus

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import org.janusgraph.core.JanusGraph
import org.janusgraph.diskstorage.configuration.{Configuration => JanusConfiguration}
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.thp.scalligraph.janus.JanusClusterManagerActor._
import org.thp.scalligraph.models.{Database, UpdatableSchema}
import org.thp.scalligraph.{InternalError, ScalligraphApplication, SingleInstance}
import play.api.{Configuration, Logger}

import javax.inject.Provider
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

class JanusDatabaseProvider(app: ScalligraphApplication) extends Provider[Database] {
  lazy val logger: Logger                      = Logger("org.thp.scalligraph.models.Database")
  val configuration: Configuration             = app.configuration
  def schemas: Seq[UpdatableSchema]            = app.schemas()
  val actorSystem: ActorSystem                 = app.actorSystem
  val singleInstance: SingleInstance           = app.singleInstance
  val coordinatedShutdown: CoordinatedShutdown = CoordinatedShutdown(actorSystem)

  implicit val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  lazy val janusClusterManager: ActorRef[Command] = JanusClusterManagerActor.getClusterManagerActor(actorSystem)

  def dropOtherConnections(db: JanusGraph): Unit = {
    val mgmt = db.openManagement()
    mgmt
      .getOpenInstances
      .asScala
      .filterNot(_.endsWith("(current)"))
      .foreach(mgmt.forceCloseInstance)
    mgmt.commit()
  }

  private trait IndexBackend {
    val name: String
    val indexLocationSetting: String

    def location(configuration: Configuration): String = configuration.get[String](s"db.janusgraph.index.search.$indexLocationSetting")

    def dbLocation(configuration: JanusConfiguration): String

    def hasChanged(db: JanusGraph, indexLocation: String): Boolean = {
      val janusConfiguration = db.asInstanceOf[StandardJanusGraph].getConfiguration.getConfiguration
      name != janusConfiguration.get(GraphDatabaseConfiguration.INDEX_BACKEND, "search") || indexLocation != dbLocation(janusConfiguration)
    }

    def updateDbConfig(db: JanusGraph, indexLocation: String): Unit
  }

  private class ElasticsearchIndexBackend extends IndexBackend {
    override val name: String                 = "elasticsearch"
    override val indexLocationSetting: String = "index-name"

    override def dbLocation(configuration: JanusConfiguration): String = configuration.get(GraphDatabaseConfiguration.INDEX_NAME, "search")

    override def updateDbConfig(db: JanusGraph, indexLocation: String): Unit = {
      val mgmt = db.openManagement()
      mgmt.set(s"index.search.backend", name)
      mgmt.set(s"index.search.$indexLocationSetting", indexLocation)
      mgmt.commit()
    }
  }

  private class LuceneIndexBackend extends IndexBackend {
    override val name: String                 = "lucene"
    override val indexLocationSetting: String = "directory"

    override def dbLocation(configuration: JanusConfiguration): String = configuration.get(GraphDatabaseConfiguration.INDEX_DIRECTORY, "search")

    override def updateDbConfig(db: JanusGraph, indexLocation: String): Unit = {
      val mgmt = db.openManagement()
      mgmt.set(s"index.search.backend", name)
      mgmt.set(s"index.search.$indexLocationSetting", indexLocation)
      mgmt.commit()
    }
  }

  override lazy val get: JanusDatabase = {
    val dbInitialisationTimeout   = configuration.get[FiniteDuration]("db.initialisationTimeout")
    implicit val timeout: Timeout = Timeout(dbInitialisationTimeout)

    val databaseInstance = configuration
      .getOptional[String]("db.janusgraph.index.search.backend")
      .collect {
        case "elasticsearch" => new ElasticsearchIndexBackend
        case "lucene"        => new LuceneIndexBackend
      }
      .map { indexBackend =>
        val indexLocation = indexBackend.location(configuration)
        val futureDb = janusClusterManager
          .ask[Result](replyTo => JoinCluster(replyTo, indexBackend.name, indexLocation))
          .map {
            case ClusterRequestInit =>
              val initialDb = JanusDatabase.openDatabase(configuration, actorSystem)
              dropOtherConnections(initialDb)
              val effectiveDB = if (indexBackend.hasChanged(initialDb, indexLocation)) {
                logger.info(s"The index backend has changed. Updating the graph configuration.")
                indexBackend.updateDbConfig(initialDb, indexLocation)
                initialDb.close()
                JanusDatabase.openDatabase(configuration, actorSystem)
              } else
                initialDb
              val db = new JanusDatabase(
                effectiveDB,
                configuration,
                actorSystem,
                singleInstance
              )
              schemas
                .toTry(_.update(db))
                .flatMap(_ => schemas.toTry(db.addSchemaIndexes))
                .fold(
                  { error =>
                    janusClusterManager ! ClusterInitFailure
                    logger.error("***********************************************************************")
                    logger.error("* Database initialisation has failed. Restart application to retry it *")
                    logger.error("***********************************************************************")
                    throw InternalError("Database initialisation failure", error)
                  },
                  { _ =>
                    janusClusterManager ! ClusterInitSuccess
                    db
                  }
                )
            case ClusterSuccessConfigurationIgnored(installedIndexBackend) =>
              logger.error("The cluster has inconsistent index configuration. Make sure application.conf are equivalent on all nodes in the cluster")
              logger.error(s"Cluster configuration: db.janusgraph.index.search.backend=$installedIndexBackend")
              logger.error(s"Local configuration (ignored): db.janusgraph.index.search.backend=$indexBackend")
              new JanusDatabase(configuration, actorSystem, singleInstance)
            case ClusterSuccess =>
              new JanusDatabase(configuration, actorSystem, singleInstance)
            case ClusterFailure =>
              logger.error("***********************************************************************")
              logger.error("* Database initialisation has failed. Restart application to retry it *")
              logger.error("***********************************************************************")
              throw InternalError("Database initialisation failure")
          }
        Await.result(futureDb, dbInitialisationTimeout)
      }
      .getOrElse {
        logger.warn("Indexer is not configured. Some queries could be very slow")
        new JanusDatabase(configuration, actorSystem, singleInstance)
      }
    coordinatedShutdown.addTask("service-stop", "Close database")(() => Future(databaseInstance.close()).map(_ => Done))
    databaseInstance
  }
}
