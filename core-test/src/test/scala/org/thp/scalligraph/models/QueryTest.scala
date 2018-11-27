package org.thp.scalligraph.models

import play.api.libs.json.Json
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import org.scalactic.Good
import org.specs2.specification.core.{Fragment, Fragments}
import org.thp.scalligraph.AppBuilder
import org.thp.scalligraph.auth.UserSrv
import org.thp.scalligraph.controllers.Field
import org.thp.scalligraph.query.AuthGraph

class QueryTest extends PlaySpecification {

  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  val userSrv: UserSrv = new DummyUserSrv

  Fragments.foreach(new DatabaseProviders().list) { dbProvider ⇒
    val app: AppBuilder = AppBuilder()
      .bindToProvider(dbProvider)
    step(setupDatabase(app)) ^ specs(dbProvider.name, app) ^ step(teardownDatabase(app))
  }

  def setupDatabase(app: AppBuilder): Unit =
    DatabaseBuilder.build(app.instanceOf[ModernSchema])(app.instanceOf[Database], userSrv.initialAuthContext)

  def teardownDatabase(app: AppBuilder): Unit = app.instanceOf[Database].drop()

  def specs(name: String, app: AppBuilder): Fragment = {

    implicit val db: Database = app.instanceOf[Database]
    new ModernSchema()
    val queryExecutor = new ModernQueryExecutor()

    s"[$name] Query executor" should {
      "execute simple query from Json" in {
        db.transaction { implicit graph ⇒
          val authGraph = AuthGraph(Some(userSrv.initialAuthContext), graph)
          val input =
            Field(Json.arr(Json.obj("_name" → "allPeople"), Json.obj("_name" → "sort", "age" → "incr"), Json.obj("_name" → "toList")))
          val result = queryExecutor.parser(input).map { query ⇒
            queryExecutor.execute(query)(authGraph).toJson
          }
          result must_=== Good(
            Json.obj("result" → Json.arr(
              Json.obj("createdBy" → "test", "label" → "Mister vadas", "name"  → "vadas", "age"  → 17),
              Json.obj("createdBy" → "test", "label" → "Mister peter", "name"  → "peter", "age"  → 25),
              Json.obj("createdBy" → "test", "label" → "Mister franck", "name" → "franck", "age" → 28),
              Json.obj("createdBy" → "test", "label" → "Mister marko", "name"  → "marko", "age"  → 29),
              Json.obj("createdBy" → "test", "label" → "Mister josh", "name"   → "josh", "age"   → 32),
              Json.obj("createdBy" → "test", "label" → "Mister marc", "name"   → "marc", "age"   → 34)
            )))
        }
      }
    }
  }
}
