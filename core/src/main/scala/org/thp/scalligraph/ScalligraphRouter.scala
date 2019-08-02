package org.thp.scalligraph

import scala.collection.immutable
import scala.util.Success

import play.api.Logger
import play.api.cache.AsyncCacheApi
import play.api.http.HttpConfiguration
import play.api.mvc.{Handler, RequestHeader, Results}
import play.api.routing.Router.Routes
import play.api.routing.sird._
import play.api.routing.{Router, SimpleRouter}

import com.google.inject.Provider
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.controllers.EntryPoint
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.query.{AuthGraph, Query, QueryExecutor}

object DebugRouter {
  lazy val logger = Logger(getClass)

  def apply(name: String, router: Router): Router = new Router {
    override def routes: Routes = new Routes {
      override def isDefinedAt(x: RequestHeader): Boolean = {
        val result = router.routes.isDefinedAt(x)
        logger.info(s"ROUTER $name $x => $result")
        result
      }
      override def apply(v1: RequestHeader): Handler = router.routes.apply(v1)
    }
    override def documentation: Seq[(String, String, String)] = router.documentation
    override def withPrefix(prefix: String): Router           = DebugRouter(s"$name.in($prefix)", router.withPrefix(prefix))
    override def toString: String                             = s"router($name)@$hashCode"
  }
}

@Singleton
class GlobalQueryExecutor @Inject()(queryExecutors: immutable.Set[QueryExecutor], cache: AsyncCacheApi) {

  def get(version: Int): QueryExecutor =
    cache.sync.getOrElseUpdate(s"QueryExecutor.$version") {
      queryExecutors
        .filter(_.versionCheck(version))
        .reduceOption(_ ++ _)
        .getOrElse(???)
    }

  def get: QueryExecutor = queryExecutors.reduce(_ ++ _)
}

@Singleton
class ScalligraphRouter @Inject()(
    httpConfig: HttpConfiguration,
    routers: immutable.Set[Router],
    entryPoint: EntryPoint,
    db: Database,
    globalQueryExecutor: GlobalQueryExecutor
) extends Provider[Router] {
  lazy val logger = Logger(getClass)

  val queryRoutes: Routes = {
    case POST(p"/api/v${int(version)}/query") =>
      val queryExecutor = globalQueryExecutor.get(version)
      entryPoint("query")
        .extract("query", queryExecutor.parser.on("query"))
        .authRoTransaction(db) { implicit request => implicit graph =>
          val authGraph = AuthGraph(request, graph)
          // macro can't be used because it is in the same module
          // val query: Query = request.body("query"
          val query: Query = request.body.list.head
          Success(Results.Ok(queryExecutor.execute(query)(authGraph).toJson))
        }
  }

  lazy val routerList: List[Router] = routers.toList
  override lazy val get: Router = {
    val prefix = httpConfig.context

    routerList
      .reduceOption(_ orElse _)
      .getOrElse(Router.empty)
      .orElse(SimpleRouter(queryRoutes))
      .withPrefix(prefix)
  }
}
