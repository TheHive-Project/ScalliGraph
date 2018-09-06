package org.thp.scalligraph.models

import org.specs2.specification.core.Fragments
import org.thp.scalligraph.auth.{AuthContext, Permission}
import play.api.test.PlaySpecification

class ModernTest extends PlaySpecification {

  case class DummyAuthContext(
      userId: String = "",
      userName: String = "",
      organisation: String = "",
      permissions: Seq[Permission] = Nil,
      requestId: String = "")
      extends AuthContext

  implicit val authContext: AuthContext = DummyAuthContext("me")

  Fragments.foreach(DatabaseProviders.list) { dbProvider ⇒
    implicit val db: Database = dbProvider.get()
    val modernSchema          = new ModernSchema
    s"[${dbProvider.name}] graph" should {
      "remove connected edge when a vertex is removed" in db.transaction { implicit graph ⇒
        // Check that marko is connected to two other people, with known level 0.5 and 1.0
        modernSchema.personSrv.get("marko").knownLevels must contain(exactly(0.5, 1.0))
        // Remove vadas who is connected to marko
        modernSchema.personSrv.get("vadas").remove()
        // Check that marko is connected to only one person
        modernSchema.personSrv.get("marko").knownLevels must contain(exactly(1.0))
      }

      "create initial values" in db.transaction { implicit graph ⇒
        modernSchema.personSrv.initSteps.name.toList must contain(exactly("marko", "vadas", "franck", "marc", "josh", "peter"))
      }
    }
  }
}
