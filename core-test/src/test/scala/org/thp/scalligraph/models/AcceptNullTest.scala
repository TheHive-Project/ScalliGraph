package org.thp.scalligraph.models

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import org.thp.scalligraph.traversal.TraversalOps
import org.thp.scalligraph.{BuildVertexEntity, EntityName}
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import scala.jdk.CollectionConverters._
import scala.util.Success

@BuildVertexEntity
case class EntityWithOptionalValue(
    name: String,
    value: Option[Int]
)

class AcceptNullTest extends PlaySpecification with TraversalOps {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  val authContext: AuthContext = AuthContextImpl("me", "", EntityName(""), "", Set.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    implicit val db: Database = dbProvider.get

    val model = Model.vertex[EntityWithOptionalValue]
    db.createSchema(model)
    db.tryTransaction { implicit graph =>
      db.createVertex(graph, authContext, model, EntityWithOptionalValue("one", Some(1)))
      db.createVertex(graph, authContext, model, EntityWithOptionalValue("two", Some(2)))
      db.createVertex(graph, authContext, model, EntityWithOptionalValue("three", Some(3)))
      db.createVertex(graph, authContext, model, EntityWithOptionalValue("null", None))
      Success(())
    }

    s"[${dbProvider.name}] Null values" should {
      "be sortable" in {
        db.transaction { implicit graph =>
          val expected = Seq("one", "two", "three", "null")
          val result   = graph.V[EntityWithOptionalValue]().sort(_.by("value", Order.asc)).value(_.name).raw.asScala.toSeq
          result must beEqualTo(expected)
        }
      }
    }
  }
}
