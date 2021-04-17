package org.thp.scalligraph

import _root_.controllers.AssetsComponents
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.stream.Materializer
import com.softwaremill.tagging._
import org.thp.scalligraph.auth.AuthSrvProvider
import org.thp.scalligraph.models.{Database, DatabaseFactory, UpdatableSchema}
import org.thp.scalligraph.query.QueryExecutor
import org.thp.scalligraph.services.{StorageFactory, StorageSrv}
import play.api.ApplicationLoader.Context
import play.api._
import play.api.cache.caffeine.CaffeineCacheComponents
import play.api.cache.{DefaultSyncCacheApi, SyncCacheApi}
import play.api.http.{FileMimeTypes, HttpConfiguration, HttpErrorHandler}
import play.api.libs.Files.{TemporaryFileCreator, TemporaryFileReaper}
import play.api.libs.concurrent.ActorSystemProvider.ApplicationShutdownReason
import play.api.libs.ws.WSClient
import play.api.libs.ws.ahc.AhcWSComponents
import play.api.mvc.{DefaultActionBuilder, EssentialFilter}
import play.api.routing.{Router, SimpleRouter}

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.{universe => ru}

trait ScalligraphModule {
  def routers: Set[Router]
  def queryExecutors: Set[QueryExecutor]
  def schemas: Set[UpdatableSchema]
  def authSrvProviders: Set[AuthSrvProvider]

  def init(): Unit = ()

  def configure(scalligraphApplication: ScalligraphApplication): ScalligraphModule = {
    _scalligraphApplication = Some(scalligraphApplication)
    init()
    this
  }
  private var _scalligraphApplication: Option[ScalligraphApplication] = None

  lazy val scalligraphApplication: ScalligraphApplication = _scalligraphApplication.getOrElse(???)
  lazy val environment: Environment                       = scalligraphApplication.context.environment
  lazy val globalSchemas: Set[UpdatableSchema] @@ Global  = scalligraphApplication.schemas

  def wireActorSingleton(props: Props, name: String): ActorRef = {
    val singletonManager =
      scalligraphApplication
        .actorSystem
        .actorOf(
          ClusterSingletonManager.props(
            singletonProps = props,
            terminationMessage = PoisonPill,
            settings = ClusterSingletonManagerSettings(scalligraphApplication.actorSystem)
          ),
          name = s"$name-Manager"
        )

    scalligraphApplication
      .actorSystem
      .actorOf(
        ClusterSingletonProxy.props(
          singletonManagerPath = singletonManager.path.toStringWithoutAddress,
          settings = ClusterSingletonProxySettings(scalligraphApplication.actorSystem)
        ),
        name = s"$name-Proxy"
      )
  }
}

trait ScalligraphApplication {
  def httpConfiguration: HttpConfiguration
  def context: Context
  def configuration: Configuration
  def application: Application
  def database: Database
  def actorSystem: ActorSystem
  def executionContext: ExecutionContext
  def syncCacheApi: SyncCacheApi
  def storageSrv: StorageSrv
  def materializer: Materializer
  def wsClient: WSClient
  def defaultActionBuilder: DefaultActionBuilder
  def singleInstance: SingleInstance
  def schemas: Set[UpdatableSchema] @@ Global
  def queryExecutors: Set[QueryExecutor] @@ Global
  def tempFileReaper: TemporaryFileReaper
  def tempFileCreator: TemporaryFileCreator
  def router: Router
  def fileMimeTypes: FileMimeTypes
  def getQueryExecutor(version: Int): QueryExecutor
  def authSrvProviders: Set[AuthSrvProvider] @@ Global
}

class ScalligraphApplicationLoader extends ApplicationLoader {
  override def load(context: Context): Application = {
    LoggerConfigurator(context.environment.classLoader).foreach {
      _.configure(context.environment, context.initialConfiguration, Map.empty)
    }
    val scalligraphComponent = new ScalligraphComponent(context)
    try {
      scalligraphComponent.router
      scalligraphComponent.application
    } catch {
      case e: Throwable =>
        scalligraphComponent.coordinatedShutdown.run(ApplicationShutdownReason).map(_ => System.exit(1))(scalligraphComponent.executionContext)
        throw e
    }
  }
}

sealed trait Global

class ScalligraphComponent(val context: Context)
    extends BuiltInComponentsFromContext(context)
    with CaffeineCacheComponents
    with AhcWSComponents
    with ScalligraphApplication
    with AssetsComponents /*with HttpFiltersComponents*/ { self =>
  val logger: Logger = Logger(getClass)

  lazy val modules: Seq[ScalligraphModule] = {
    val rm: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    configuration
      .get[Seq[String]]("scalligraph.modules")
      .flatMap { moduleName =>
        rm.reflectModule(rm.staticModule(moduleName)).instance match {
          case obj: ScalligraphModule =>
            logger.info(s"Loading module ${obj.getClass.getSimpleName.stripSuffix("$")}")
            Some(obj.configure(this))
          case obj =>
            logger.error(s"Fail to load module ${obj.getClass.getSimpleName}")
            None
        }
      }
  }

  override lazy val singleInstance: SingleInstance = new SingleInstance(configuration.get[Seq[String]]("akka.cluster.seed-nodes").isEmpty) {
    if (value) {
      logger.info("Initialising cluster")
      val cluster = Cluster(actorSystem)
      cluster.join(cluster.system.provider.getDefaultAddress)
    }
  }

  override lazy val schemas: Set[UpdatableSchema] @@ Global = modules.flatMap(_.schemas).toSet.taggedWith[Global]

  override lazy val syncCacheApi: SyncCacheApi = defaultCacheApi.sync match {
    case sync: SyncCacheApi => sync
    case _                  => new DefaultSyncCacheApi(defaultCacheApi)
  }

  override lazy val router: Router = modules
    .flatMap(_.routers.zipWithIndex)
    .sortBy(_._2)
    .map(_._1)
    .reduceOption(_ orElse _)
    .getOrElse(Router.empty)
    .orElse {
      SimpleRouter {
        case _ => ???
      }
    }
//  val authRoutes: Routes = {
//    case _ =>
//      actionBuilder.async { request =>
//        authSrv
//          .actionFunction(defaultAction)
//          .invokeBlock(
//            request,
//            (_: AuthenticatedRequest[AnyContent]) =>
//              if (request.path.endsWith("/ssoLogin"))
//                Future.successful(Results.Redirect(prefix))
//              else
//                Future.failed(NotFoundError(request.path))
//          )
//      }
//  }

  override lazy val httpErrorHandler: HttpErrorHandler = ErrorHandler

  override lazy val queryExecutors: Set[QueryExecutor] @@ Global = modules.flatMap(_.queryExecutors).toSet.taggedWith[Global]

  override def httpFilters: Seq[EssentialFilter] = Seq(new AccessLogFilter()(executionContext))

  override lazy val database: Database     = DatabaseFactory.apply(this)
  override lazy val storageSrv: StorageSrv = StorageFactory.apply(this)

  override def getQueryExecutor(version: Int): QueryExecutor =
    syncCacheApi.getOrElseUpdate(s"QueryExecutor.$version") {
      queryExecutors
        .filter(_.versionCheck(version))
        .reduceOption(_ ++ _)
        .getOrElse(throw BadRequestError(s"No available query executor for version $version"))
    }

  override lazy val authSrvProviders: Set[AuthSrvProvider] @@ Global = modules.flatMap(_.authSrvProviders).toSet.taggedWith[Global]
}
