package org.thp.scalligraph.models

import gremlin.scala.{GremlinScala, Vertex}
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}
import org.specs2.mock.Mockito
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.VertexEntity
import org.thp.scalligraph.auth.{AuthContext, UserSrv}
import org.thp.scalligraph.services.VertexSrv

@VertexEntity
case class EntityWithSeq(name: String, valueList: Seq[String], valueSet: Set[String])

class CardinalityTest extends PlaySpecification with Mockito {

  val userSrv: UserSrv                  = DummyUserSrv()
  implicit val authContext: AuthContext = userSrv.initialAuthContext
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  Fragments.foreach(DatabaseProviders.list) { dbProvider ⇒
    implicit val db: Database = dbProvider.get()
    db.createSchema(db.getModel[EntityWithSeq])
    val entityWithSeqSrv: VertexSrv[EntityWithSeq, VertexSteps[EntityWithSeq]] = new VertexSrv[EntityWithSeq, VertexSteps[EntityWithSeq]] {
      override def steps(raw: GremlinScala[Vertex]): VertexSteps[EntityWithSeq] = new VertexSteps[EntityWithSeq](raw)
    }

    s"[${dbProvider.name}] entity" should {
      "create with empty list and set" in db.transaction { implicit graph ⇒
        val initialEntity                            = EntityWithSeq("The answer", Seq.empty, Set.empty)
        val createdEntity: EntityWithSeq with Entity = entityWithSeqSrv.create(initialEntity)
        createdEntity._id must_!== null
        initialEntity must_=== createdEntity
        createdEntity must_=== entityWithSeqSrv.getOrFail(createdEntity._id)
      }

      "create and get entities with list property" in db.transaction { implicit graph ⇒
        val initialEntity                            = EntityWithSeq("list", Seq("1", "2", "3"), Set.empty)
        val createdEntity: EntityWithSeq with Entity = entityWithSeqSrv.create(initialEntity)
        initialEntity must_=== createdEntity
        createdEntity must_=== entityWithSeqSrv.getOrFail(createdEntity._id)
      }

      "create and get entities with set property" in db.transaction { implicit graph ⇒
        val initialEntity                            = EntityWithSeq("list", Seq.empty, Set("a", "b", "c"))
        val createdEntity: EntityWithSeq with Entity = entityWithSeqSrv.create(initialEntity)
        initialEntity must_=== createdEntity
        createdEntity must_=== entityWithSeqSrv.getOrFail(createdEntity._id)
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
