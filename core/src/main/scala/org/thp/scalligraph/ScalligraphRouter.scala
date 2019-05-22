package org.thp.scalligraph

import scala.collection.immutable
import scala.util.Success

import play.api.http.HttpConfiguration
import play.api.mvc.Results
import play.api.routing.Router.Routes
import play.api.routing.sird._
import play.api.routing.{Router, SimpleRouter}

import com.google.inject.Provider
import javax.inject.Inject
import org.thp.scalligraph.controllers.EntryPoint
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.query.{AuthGraph, Query, QueryExecutor}

class ScalligraphRouter @Inject()(
    httpConfig: HttpConfiguration,
    routers: immutable.Set[Router],
    entryPoint: EntryPoint,
    db: Database,
    queryExecutors: immutable.Set[QueryExecutor]
) extends Provider[Router] {

  val queryRoutes: Routes = {
    case POST(p"/api/v${int(version)}/query") ⇒
      val queryExecutor = queryExecutors
        .filter(_.versionCheck(version))
        .reduceOption(_ ++ _)
        .getOrElse(???)
      entryPoint("query")
        .extract('query, queryExecutor.parser.on("query"))
        .authTransaction(db) { implicit request ⇒ implicit graph ⇒
          val authGraph = AuthGraph(request, graph)
          // macro can't be used because it is in the same module
          // val query: Query = request.body('query
          val query: Query = request.body.list.head
          Success(Results.Ok(queryExecutor.execute(query)(authGraph).toJson))
        }
  }

  lazy val routerList: List[Router] = routers.toList
  override lazy val get: Router = {
    val prefix = httpConfig.context

    val router = new SimpleRouter {
      override def routes: Routes =
        routerList
          .map(_.routes)
          .reduceOption(_ orElse _)
          .getOrElse(PartialFunction.empty)
          .orElse(queryRoutes)
      override def documentation: Seq[(String, String, String)] = routerList.flatMap(_.documentation)
    }
    router.withPrefix(prefix)
  }
}
