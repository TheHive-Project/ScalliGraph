package org.thp.scalligraph.models

import org.scalactic.Good
import org.specs2.mock.Mockito
import org.specs2.specification.core.{Fragment, Fragments}
import org.thp.scalligraph.AppBuilder
import org.thp.scalligraph.auth.UserSrv
import org.thp.scalligraph.controllers.Field
import org.thp.scalligraph.query.AuthGraph
import play.api.libs.json.Json
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

class QueryTest extends PlaySpecification with Mockito {

  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  val userSrv: UserSrv = new DummyUserSrv

  Fragments.foreach(new DatabaseProviders().list) { dbProvider ⇒
    val app: AppBuilder = AppBuilder()
      .bindToProvider(dbProvider)
      .addConfiguration("play.modules.disabled = [org.thp.scalligraph.ScalligraphModule]")
    step(setupDatabase(app)) ^ specs(dbProvider.name, app) ^ step(teardownDatabase(app))
  }

  def setupDatabase(app: AppBuilder): Unit =
    DatabaseBuilder.build(app.instanceOf[ModernSchema])(app.instanceOf[Database], userSrv.initialAuthContext)

  def teardownDatabase(app: AppBuilder): Unit = app.instanceOf[Database].drop()

  def specs(name: String, app: AppBuilder): Fragment = {

    implicit val db: Database = app.instanceOf[Database]
    val queryExecutor         = new ModernQueryExecutor()

    s"[$name] Query executor" should {
      "execute simple query from Json" in {
        db.transaction { implicit graph ⇒
          val authGraph = AuthGraph(Some(userSrv.initialAuthContext), graph)
          val input =
            Field(
              Json.arr(
                Json.obj("_name" → "allPeople"),
                Json.obj("_name" → "sort", "_fields" → Json.arr(Json.obj("age" → "incr"))),
                Json.obj("_name" → "toList")))
          val result = queryExecutor.parser(input).map { query ⇒
            queryExecutor.execute(query)(authGraph).toJson
          }
          result must_=== Good(
            Json.obj("result" → Json.arr(
              Json.obj("createdBy" → "test", "label" → "Mister vadas", "name"  → "vadas", "age"  → 27),
              Json.obj("createdBy" → "test", "label" → "Mister franck", "name" → "franck", "age" → 28),
              Json.obj("createdBy" → "test", "label" → "Mister marko", "name"  → "marko", "age"  → 29),
              Json.obj("createdBy" → "test", "label" → "Mister josh", "name"   → "josh", "age"   → 32),
              Json.obj("createdBy" → "test", "label" → "Mister marc", "name"   → "marc", "age"   → 34),
              Json.obj("createdBy" → "test", "label" → "Mister peter", "name"  → "peter", "age"  → 35)
            )))
        }
      }

      "execute aggregation query" in {
        db.transaction { implicit graph ⇒
          val authGraph = AuthGraph(Some(userSrv.initialAuthContext), graph)
          val input = Field(
            Json.arr(
              Json.obj("_name" → "allPeople"),
              Json.obj("_name" → "aggregation", "_agg" → "field", "_field" → "age", "_select" → Json.arr(Json.obj("_agg" → "count")))
            ))
          val result = queryExecutor.parser(input).map { query ⇒
            queryExecutor.execute(query)(authGraph).toJson
          }
          result must_== Good(
            Json.obj(
              "32" → Json.obj("count" → 1),
              "27" → Json.obj("count" → 1),
              "34" → Json.obj("count" → 1),
              "35" → Json.obj("count" → 1),
              "28" → Json.obj("count" → 1),
              "29" → Json.obj("count" → 1)
            ))
        }
      }

      "execute aggregation query 2" in {
        db.transaction { implicit graph ⇒
          val authGraph = AuthGraph(Some(userSrv.initialAuthContext), graph)
          val input = Field(
            Json.arr(
              Json.obj("_name" → "allSoftware"),
              Json.obj("_name" → "aggregation", "_agg" → "field", "_field" → "lang", "_select" → Json.arr(Json.obj("_agg" → "count")))
            ))
          val result = queryExecutor.parser(input).map { query ⇒
            queryExecutor.execute(query)(authGraph).toJson
          }
          result must_== Good(
            Json.obj(
              "java" → Json.obj("count" → 2)
            ))
        }
      }
    }
  }
}
