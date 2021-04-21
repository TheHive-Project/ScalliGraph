package org.thp.scalligraph.controllers

import org.specs2.concurrent.ExecutionEnv
import org.specs2.mock.Mockito
import org.thp.scalligraph.ErrorHandler
import org.thp.scalligraph.auth.AuthSrv
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.Json
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.mvc.{AnyContentAsJson, DefaultActionBuilder, Results}
import play.api.test.{FakeRequest, Helpers, PlaySpecification}
import play.api.{Application, Configuration, Environment}

import scala.util.Success

class ControllerTest(implicit executionEnv: ExecutionEnv) extends PlaySpecification with Mockito {
  lazy val app: Application = new GuiceApplicationBuilder().build()

  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  "controller" should {

    "extract simple class from HTTP request" in {

      val actionBuilder = DefaultActionBuilder(Helpers.stubBodyParser())
      val entrypoint    = new Entrypoint(mock[AuthSrv], actionBuilder, ErrorHandler, executionEnv.ec)

      val action = entrypoint("model extraction")
        .extract("simpleClass", FieldsParser[SimpleClassForFieldsParserMacroTest]) { req =>
          val simpleClass = req.body("simpleClass")
          simpleClass must_=== SimpleClassForFieldsParserMacroTest("myName", 44)
          Success(Results.Ok("ok"))
        }

      val request  = FakeRequest("POST", "/api/simple_class").withBody(AnyContentAsJson(Json.obj("name" -> "myName", "value" -> 44)))
      val result   = action(request)
      val bodyText = contentAsString(result)
      bodyText must be equalTo "ok"
    }
  }
}
