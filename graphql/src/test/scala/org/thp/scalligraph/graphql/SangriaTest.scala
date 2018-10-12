package org.thp.scalligraph.graphql

import java.io.FileNotFoundException

import gremlin.scala._
import org.specs2.matcher.MatchResult
import org.thp.scalligraph.auth.{AuthContext, Permission}
import org.thp.scalligraph.janus.JanusDatabase
import org.thp.scalligraph.models.{ModernQueryExecutor, ModernSchema}
import org.thp.scalligraph.query.AuthGraph
import play.api.libs.json.{JsObject, JsValue, Json}
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}
import sangria.ast.Document
import sangria.execution.Executor
import sangria.marshalling.playJson._
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer
import sangria.schema.{Schema ⇒ SangriaSchema}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

class SangriaTest extends PlaySpecification {
  case class DummyAuthContext(
      userId: String = "",
      userName: String = "",
      organisation: String = "",
      permissions: Seq[Permission] = Nil,
      requestId: String = "")
      extends AuthContext

  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  implicit val authContext: AuthContext = DummyAuthContext("me")
  implicit val db: JanusDatabase        = new JanusDatabase()
  val modernSchema                      = new ModernSchema
  val executor                          = new ModernQueryExecutor

  implicit val schema: SangriaSchema[AuthGraph, Unit] = SchemaGenerator(executor)

  def executeQuery(query: Document, expected: JsValue, variables: JsValue = JsObject.empty)(
      implicit graph: Graph,
      schema: SangriaSchema[AuthGraph, Unit]): MatchResult[_] = {
    import org.thp.scalligraph.services.Implicits.singleThreadedExecutionContext

    val futureResult = Executor.execute(schema, query, AuthGraph(Some(authContext), graph), variables = variables)
    val result       = Await.result(futureResult, 10.seconds)
    result must_=== expected
  }

  def readResource(resource: String): Try[String] =
    Try(Source.fromResource(resource).mkString)
      .recoverWith { case NonFatal(_) ⇒ Failure(new FileNotFoundException(resource)) }

  def executeQueryFile(testName: String, variables: JsObject = JsObject.empty)(
      implicit graph: Graph,
      schema: SangriaSchema[AuthGraph, Unit]): MatchResult[_] = {
    val query    = QueryParser.parse(readResource(s"graphql/$testName.graphql").get).get
    val expected = Json.parse(readResource(s"graphql/$testName.expected.json").get)
    val vars     = readResource(s"graphql/$testName.vars.json").fold(_ ⇒ variables, Json.parse)
    executeQuery(query = query, expected = expected, variables = vars)
  }

  "Modern graph" should {
    "finds all persons" in db.transaction { implicit graph ⇒
      val personSteps = modernSchema.personSrv.initSteps
      val r           = personSteps.toSet.map(_.name)
      r must_=== Set("marko", "vadas", "josh", "peter", "marc", "franck")
    }

    "have GraphQL schema" in db.transaction { implicit graph ⇒
      val schemaStr = SchemaRenderer.renderSchema(schema)
      println(s"new modern graphql schema is:\n$schemaStr")

      schemaStr must_!== ""
    }

    "execute simple query" in db.transaction { implicit graph ⇒
      executeQueryFile("simpleQuery")
    }

    "filter entity using query object" in db.transaction { implicit graph ⇒
      executeQueryFile("queryWithFilterObject")
    }

    "filter entity using query object with boolean operator" in db.transaction { implicit graph ⇒
      executeQueryFile("queryWithBooleanOperators")
    }

    "return several attributes" in db.transaction { implicit graph ⇒
      executeQueryFile("queryWithSeveralAttributes")
    }

    "execute complex query" in db.transaction { implicit graph ⇒
      executeQueryFile("complexQuery")
    }
  }
}