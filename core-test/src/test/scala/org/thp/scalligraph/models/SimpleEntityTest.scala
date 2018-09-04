package org.thp.scalligraph.models

import play.api.{Configuration, Environment}
import play.api.libs.logback.LogbackLoggerConfigurator

import org.apache.tinkerpop.gremlin.structure.T
import org.specs2.mock.Mockito
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.VertexEntity
import org.thp.scalligraph.auth.{AuthContext, UserSrv}
import org.thp.scalligraph.services.VertexSrv
import play.api.test.PlaySpecification

@VertexEntity
case class MyEntity(name: String, value: Int)

object MyEntity {
  val initialValues = Seq(MyEntity("ini1", 1), MyEntity("ini1", 2))
}

class SimpleEntityTest extends PlaySpecification with Mockito {

  val userSrv: UserSrv                  = DummyUserSrv()
  implicit val authContext: AuthContext = userSrv.initialAuthContext
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  Fragments.foreach(DatabaseProviders.list) { dbProvider ⇒
    implicit val db: Database = dbProvider.get()
    db.createSchema(db.getModel[MyEntity])
    val myEntitySrv: VertexSrv[MyEntity] = new VertexSrv[MyEntity]

    s"[${dbProvider.name}] simple entity" should {
      "create" in db.transaction { implicit graph ⇒
        val createdEntity: MyEntity with Entity = myEntitySrv.create(MyEntity("The answer", 42))
        createdEntity._id must_!== null
      }

      "fail to create if data is invalid" in db.transaction { implicit graph ⇒
        graph
          .addVertex(T.label, myEntitySrv.model.label, "name", "plop", "value", 1: java.lang.Integer, "_createdBy", "nobody")
          .id
          .toString must throwA[Exception]
      }

      "create and get entities" in db.transaction { implicit graph ⇒
        val createdEntity: MyEntity with Entity = myEntitySrv.create(MyEntity("e^π", -1))
        val e                                   = myEntitySrv.getOrFail(createdEntity._id)
        e.name must_=== "e^π"
        e.value must_=== -1
        e._createdBy must_=== "test"
      }

      "update an entity" in db.transaction { implicit graph ⇒
        val id = myEntitySrv.create(MyEntity("super", 7))._id
        myEntitySrv.update(id, "value", 8)

        myEntitySrv.getOrFail(id).value must_=== 8
      }
    }
  }
}
