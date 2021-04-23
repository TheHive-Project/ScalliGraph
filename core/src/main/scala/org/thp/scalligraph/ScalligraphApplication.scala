package org.thp.scalligraph

import _root_.controllers.{Assets, AssetsComponents}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.stream.Materializer
import com.softwaremill.macwire.Module
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
import play.api.routing.Router

import java.io.File
import scala.concurrent.ExecutionContext
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal
trait ScalligraphModule {
  def init(): Unit = ()
}

trait ActorSingletonUtils {
  def wireActorSingleton(actorSystem: ActorSystem, props: Props, name: String): ActorRef = {
    val singletonManager =
      actorSystem
        .actorOf(
          ClusterSingletonManager.props(
            singletonProps = props,
            terminationMessage = PoisonPill,
            settings = ClusterSingletonManagerSettings(actorSystem)
          ),
          name = s"$name-Manager"
        )

    actorSystem
      .actorOf(
        ClusterSingletonProxy.props(
          singletonManagerPath = singletonManager.path.toStringWithoutAddress,
          settings = ClusterSingletonProxySettings(actorSystem)
        ),
        name = s"$name-Proxy"
      )
  }
}

@Module
trait ScalligraphApplication {
  def loadedModules: Seq[ScalligraphModule]
  def getModule[M: ClassTag]: M
  def injectModule(module: ScalligraphModule): Unit
  def httpConfiguration: HttpConfiguration
  val context: Context
  def configuration: Configuration
  def application: Application
  def database: Database
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext
  def syncCacheApi: SyncCacheApi
  def storageSrv: StorageSrv
  def materializer: Materializer
  def wsClient: WSClient
  def defaultActionBuilder: DefaultActionBuilder
  def singleInstance: SingleInstance
  def schemas: SemiMutableSeq[UpdatableSchema]
  def queryExecutors: SemiMutableSeq[QueryExecutor]
  def tempFileReaper: TemporaryFileReaper
  def tempFileCreator: TemporaryFileCreator
  def routers: SemiMutableSeq[Router]
  def fileMimeTypes: FileMimeTypes
  def getQueryExecutor(version: Int): QueryExecutor
  def authSrvProviders: SemiMutableSeq[AuthSrvProvider]
  def assets: Assets
}

class ScalligraphApplicationLoader extends ApplicationLoader {
  override def load(context: Context): Application = {
    LoggerConfigurator(context.environment.classLoader).foreach {
      _.configure(context.environment, context.initialConfiguration, Map.empty)
    }
    val app = new ScalligraphApplicationImpl(context)
    try {
      app.init()
      app.router
      app.application
    } catch {
      case e: Throwable =>
        app.coordinatedShutdown.run(ApplicationShutdownReason).map(_ => System.exit(1))(app.executionContext)
        throw e
    }
  }
}

@Module
class ScalligraphApplicationImpl(val context: Context)
    extends BuiltInComponentsFromContext(context)
    with CaffeineCacheComponents
    with AhcWSComponents
    with AssetsComponents
    with ScalligraphApplication /*with HttpFiltersComponents*/ {

  def this(rootDir: File, classLoader: ClassLoader, mode: Mode, initialSettings: Map[String, AnyRef] = Map.empty[String, AnyRef]) =
    this(
      ApplicationLoader.Context.create(Environment(rootDir, classLoader, mode), initialSettings)
    )

  val logger: Logger = Logger(getClass)

  private var _loadedModules: Seq[ScalligraphModule] = Seq.empty

  override def getModule[M: ClassTag]: M = {
    val moduleClass = classTag[M].runtimeClass
    loadedModules.find(m => moduleClass.isAssignableFrom(m.getClass)).getOrElse(???).asInstanceOf[M]
  }

  override def injectModule(module: ScalligraphModule): Unit = _loadedModules = _loadedModules :+ module

  override def loadedModules: Seq[ScalligraphModule] = _loadedModules

  lazy val singleInstance: SingleInstance = new SingleInstance(configuration.get[Seq[String]]("akka.cluster.seed-nodes").isEmpty) {
    if (value) {
      logger.info("Initialising cluster")
      val cluster = Cluster(actorSystem)
      cluster.join(cluster.system.provider.getDefaultAddress)
    }
  }

  lazy val schemas: SemiMutableSeq[UpdatableSchema] = SemiMutableSeq[UpdatableSchema]

  lazy val syncCacheApi: SyncCacheApi = defaultCacheApi.sync match {
    case sync: SyncCacheApi => sync
    case _                  => new DefaultSyncCacheApi(defaultCacheApi)
  }

  val routers: SemiMutableSeq[Router] = SemiMutableSeq[Router]
  override lazy val router: Router = routers()
    .reduceOption(_ orElse _)
    .getOrElse(Router.empty)
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

  lazy val queryExecutors: SemiMutableSeq[QueryExecutor] = SemiMutableSeq[QueryExecutor]

  override def httpFilters: Seq[EssentialFilter] = Seq(new AccessLogFilter()(executionContext))

  lazy val database: Database     = DatabaseFactory.apply(this)
  lazy val storageSrv: StorageSrv = StorageFactory.apply(this)

  def getQueryExecutor(version: Int): QueryExecutor =
    syncCacheApi.getOrElseUpdate(s"QueryExecutor.$version") {
      queryExecutors()
        .filter(_.versionCheck(version))
        .reduceOption(_ ++ _)
        .getOrElse(throw BadRequestError(s"No available query executor for version $version"))
    }

  lazy val authSrvProviders: SemiMutableSeq[AuthSrvProvider] = SemiMutableSeq[AuthSrvProvider]

  def init(): Unit = {
    LoggerConfigurator(environment.classLoader).foreach {
      _.configure(environment, context.initialConfiguration, Map.empty)
    }

    configuration
      .get[Seq[String]]("scalligraph.modules")
      .foreach { moduleName =>
        logger.info(s"Loading module $moduleName")
        try {
          val module = context
            .environment
            .classLoader
            .loadClass(moduleName)
            .getConstructor(classOf[ScalligraphApplication])
            .newInstance(this)
            .asInstanceOf[ScalligraphModule]
          injectModule(module)
          module.init()
        } catch {
          case NonFatal(e) => logger.error(s"Fail to load module $moduleName", e)
        }
      }
    router
    ()
  }
}

class SemiMutableSeq[T] private (private var _values: Seq[() => T] = Seq.empty) extends Seq[T] {
  private var initPhase: Boolean = true

  def +=(v: => T): Unit =
    if (initPhase) _values = _values :+ (() => v)
    else throw new IllegalStateException("SemiMutableSeq initialisation is over", firstInit)

  def apply(): Seq[T] = values

  private var firstInit: Throwable = _

  lazy val values: Seq[T] = {
    initPhase = false
    try ???
    catch {
      case t: Throwable => firstInit = t
    }
    _values.map(_.apply())
  }

  override def length: Int           = apply().length
  override def apply(idx: Int): T    = apply().apply(idx)
  override def iterator: Iterator[T] = apply().iterator
}

object SemiMutableSeq {
  def apply[T]                                    = new SemiMutableSeq[T]
  def apply[T](vv: (() => T)*): SemiMutableSeq[T] = new SemiMutableSeq[T](vv)
}
