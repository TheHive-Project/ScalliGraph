package org.thp.scalligraph.models

import scala.util.Try

import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import gremlin.scala.{Graph, GremlinScala, Vertex}
import org.specs2.mock.Mockito
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.VertexEntity
import org.thp.scalligraph.auth.{AuthContext, UserSrv}
import org.thp.scalligraph.services.VertexSrv

@VertexEntity
case class MyEntity(name: String, value: Int)

object MyEntity {
  val initialValues: Seq[MyEntity] = Seq(MyEntity("ini1", 1), MyEntity("ini1", 2))
}

class MyEntitySrv(implicit db: Database) extends VertexSrv[MyEntity, VertexSteps[MyEntity]] {
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): VertexSteps[MyEntity]         = new VertexSteps[MyEntity](raw)
  def create(e: MyEntity)(implicit graph: Graph, authContext: AuthContext): Try[MyEntity with Entity] = createEntity(e)
}

class SimpleEntityTest extends PlaySpecification with Mockito {

  val userSrv: UserSrv                  = DummyUserSrv()
  implicit val authContext: AuthContext = userSrv.getSystemAuthContext
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    implicit val db: Database = dbProvider.get()
    db.createSchema(db.getModel[MyEntity])
    val myEntitySrv: MyEntitySrv = new MyEntitySrv

    s"[${dbProvider.name}] simple entity" should {
      "create" in db.transaction { implicit graph =>
        myEntitySrv.create(MyEntity("The answer", 42)) must beSuccessfulTry.which { createdEntity =>
          createdEntity._id must_!== null
        }
      }

      "create and get entities" in db.transaction { implicit graph =>
        myEntitySrv
          .create(MyEntity("e^π", -1))
          .flatMap { createdEntity =>
            myEntitySrv.getOrFail(createdEntity._id)
          } must beSuccessfulTry.which { e: MyEntity with Entity =>
          e.name must_=== "e^π"
          e.value must_=== -1
          e._createdBy must_=== "admin"
        }
      }

      "update an entity" in db.transaction { implicit graph =>
        myEntitySrv.create(MyEntity("super", 7)) must beASuccessfulTry.which { entity =>
          myEntitySrv.getByIds(entity._id).update("value" -> 8) must beSuccessfulTry
          myEntitySrv.getOrFail(entity._id) must beSuccessfulTry.which((_: MyEntity with Entity).value must_=== 8)
        }
      }
    }
  }
}
