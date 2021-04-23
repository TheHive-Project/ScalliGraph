package org.thp.scalligraph.models

import org.specs2.specification.core.{Fragment, Fragments}
import org.thp.scalligraph.EntityName
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import org.thp.scalligraph.traversal.TraversalOps
import play.api.test.PlaySpecification

import scala.util.Try

class ModernTest extends PlaySpecification with TraversalOps {

  implicit val authContext: AuthContext = AuthContextImpl("me", "", EntityName(""), "", Set.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    step(setupDatabase(dbProvider.get)) ^ specs(dbProvider.name, dbProvider.get) ^ step(teardownDatabase(dbProvider.get))
  }

  def setupDatabase(db: Database): Try[Unit] =
    ModernDatabaseBuilder.build(db)(authContext)

  def teardownDatabase(db: Database): Unit = db.drop()

  def specs(name: String, db: Database): Fragment = {
    val personSrv = new PersonSrv

    s"[$name] graph" should {
//      "remove connected edge when a vertex is removed" in db.transaction { implicit graph =>
//        // Check that marko is connected to two other people, with known level 0.5 and 1.0
//        personSrv.get("marko").knownLevels must contain(exactly(0.5, 1.0))
//        // Remove vadas who is connected to marko
//        personSrv.get("vadas").remove()
//        // Check that marko is connected to only one person
//        personSrv.get("marko").knownLevels must contain(exactly(1.0))
//      }

      "create initial values" in db.roTransaction { implicit graph =>
        personSrv.startTraversal.toSeq.map(_.name) must contain(exactly("marko", "vadas", "franck", "marc", "josh", "peter"))
      }
    }
  }
}
