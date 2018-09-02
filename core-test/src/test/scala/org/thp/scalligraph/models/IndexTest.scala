package org.thp.scalligraph.models

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import org.specs2.specification.core.Fragments
import org.thp.scalligraph.VertexEntity

@DefineIndex(IndexType.unique, "name")
@VertexEntity
case class EntityWithUniqueName(name: String, value: Int)

class IndexTest extends PlaySpecification {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  Fragments.foreach(DatabaseProviders.list) { dbProvider ⇒
    implicit val db: Database = dbProvider.get()
    val model                 = Model.vertex[EntityWithUniqueName]
    db.createSchema(model)

    s"[${dbProvider.name}] Creating duplicate entries on unique index constraint" should {
      "throw an exception in the same transaction" in {
        db.transaction { implicit graph ⇒
          model.create(EntityWithUniqueName("singleTransaction", 1))
          model.create(EntityWithUniqueName("singleTransaction", 2))
        } must throwA[Exception]
      }

      "throw an exception in the different transactions" in {
        db.transaction { implicit graph ⇒
          model.create(EntityWithUniqueName("differentTransaction", 1))
        }
        db.transaction { implicit graph ⇒
          model.create(EntityWithUniqueName("differentTransaction", 2))
        } must throwA[Exception]
      }

      "throw an exception in overlapped transactions" in {
        // This test sometimes fails on JanusGraph
        // https://github.com/JanusGraph/janusgraph/issues/1107
        def synchronizedElementCreation(name: String, waitBeforeCreate: Future[Unit], waitBeforeCommit: Future[Unit]): Future[Unit] =
          Future {
            db.transaction { implicit graph ⇒
              Await.result(waitBeforeCreate, 2.seconds)
              model.create(EntityWithUniqueName(name, 1))
              Await.result(waitBeforeCommit, 2.seconds)
            }
          }

        val waitBeforeCreate = Promise[Unit]
        val waitBeforeCommit = Promise[Unit]
        val f1               = synchronizedElementCreation("overlappedTransaction", waitBeforeCreate.future, waitBeforeCommit.future)
        val f2               = synchronizedElementCreation("overlappedTransaction", waitBeforeCreate.future, waitBeforeCommit.future)
        waitBeforeCreate.success(())
        waitBeforeCommit.success(())
        Await.result(f1.flatMap(_ ⇒ f2), 5.seconds) must throwA[Exception]
      }
    }
  }
}
