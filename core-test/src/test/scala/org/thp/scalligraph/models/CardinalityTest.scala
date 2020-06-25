package org.thp.scalligraph.models

import gremlin.scala.{Graph, GremlinScala, Key, Vertex}
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.VertexEntity
import org.thp.scalligraph.auth.{AuthContext, UserSrv}
import org.thp.scalligraph.services.VertexSrv
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import scala.util.{Success, Try}

@VertexEntity
case class EntityWithSeq(name: String, valueList: Seq[String], valueSet: Set[String])

class EntityWithSeqSrv(implicit db: Database) extends VertexSrv[EntityWithSeq, VertexSteps[EntityWithSeq]] {
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): VertexSteps[EntityWithSeq] = new VertexSteps[EntityWithSeq](raw)

  def create(e: EntityWithSeq)(implicit graph: Graph, authContext: AuthContext): Try[EntityWithSeq with Entity] = createEntity(e)

  def getFromKey(key: String, value: String)(implicit graph: Graph): Try[EntityWithSeq] =
    new VertexSteps[EntityWithSeq](initSteps.raw.has(Key(key) of value)).getOrFail()
}

class CardinalityTest extends PlaySpecification {

  val userSrv: UserSrv                  = DummyUserSrv()
  implicit val authContext: AuthContext = userSrv.getSystemAuthContext
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    implicit val db: Database = dbProvider.get()
    db.createSchema(db.getModel[EntityWithSeq])
    db.addSchemaIndexes(db.getModel[EntityWithSeq])
    val entityWithSeqSrv: EntityWithSeqSrv = new EntityWithSeqSrv

    s"[${dbProvider.name}] entity" should {
      "create with empty list and set" in db.transaction { implicit graph =>
        val initialEntity = EntityWithSeq("The answer", Seq.empty, Set.empty)
        entityWithSeqSrv.create(initialEntity) must beSuccessfulTry.which { createdEntity =>
          createdEntity._id must_!== null
          initialEntity must_=== createdEntity
          entityWithSeqSrv.getOrFail(createdEntity._id) must beSuccessfulTry(createdEntity)
        }
      }

      "create and get entities with list property" in db.transaction { implicit graph =>
        val initialEntity = EntityWithSeq("list", Seq("1", "2", "3"), Set.empty)
        entityWithSeqSrv.create(initialEntity) must beSuccessfulTry.which { createdEntity =>
          initialEntity must_=== createdEntity
          entityWithSeqSrv.getOrFail(createdEntity._id) must beSuccessfulTry(createdEntity)
        }
      }

      "create and get entities with set property" in db.transaction { implicit graph =>
        val initialEntity = EntityWithSeq("list", Seq.empty, Set("a", "b", "c"))
        entityWithSeqSrv.create(initialEntity) must beSuccessfulTry.which { createdEntity =>
          initialEntity must_=== createdEntity
          entityWithSeqSrv.getOrFail(createdEntity._id) must_=== Success(createdEntity)
        }
      }

      "be searchable from its list property" in db.transaction { implicit graph =>
        val initialEntity = EntityWithSeq("list", Seq("1", "2", "3"), Set.empty)
        entityWithSeqSrv.create(initialEntity) must beSuccessfulTry.which { createdEntity =>
          entityWithSeqSrv.getFromKey("valueList", "1") must beSuccessfulTry(createdEntity)
        // This test fails with OrientDB : https://github.com/orientechnologies/orientdb-gremlin/issues/120
        }
      }

//      "update an entity" in db.transaction { implicit graph ⇒
//        val id = entityWithSeqSrv.create(EntityWithSeq("super", 7))._id
//        entityWithSeqSrv.update(id, "value", 8)
//
//        entityWithSeqSrv.getOrFail(id).value must_=== 8
//      }
    }
  }
}
