package org.thp.scalligraph.models

import org.specs2.specification.core.{Fragment, Fragments}
import org.thp.scalligraph.auth.{AuthContext, Permission}
import play.api.test.PlaySpecification

import org.thp.scalligraph.AppBuilder

case class DummyAuthContext(
    userId: String = "",
    userName: String = "",
    organisation: String = "",
    permissions: Seq[Permission] = Nil,
    requestId: String = "")
    extends AuthContext

class ModernTest extends PlaySpecification {

  implicit val authContext: AuthContext = DummyAuthContext("me")

  Fragments.foreach(new DatabaseProviders().list) { dbProvider ⇒
    val app: AppBuilder = AppBuilder()
      .bindToProvider(dbProvider)
    step(setupDatabase(app)) ^ specs(dbProvider.name, app) ^ step(teardownDatabase(app))
  }

  def setupDatabase(app: AppBuilder): Unit =
    DatabaseBuilder.build(app.instanceOf[ModernSchema])(app.instanceOf[Database], authContext)

  def teardownDatabase(app: AppBuilder): Unit = app.instanceOf[Database].drop()

  def specs(name: String, app: AppBuilder): Fragment = {
    implicit val db: Database = app.instanceOf[Database]
    val personSrv             = app.instanceOf[PersonSrv]

    s"[$name] graph" should {
//      "remove connected edge when a vertex is removed" in db.transaction { implicit graph ⇒
//        // Check that marko is connected to two other people, with known level 0.5 and 1.0
//        personSrv.get("marko").knownLevels must contain(exactly(0.5, 1.0))
//        // Remove vadas who is connected to marko
//        personSrv.get("vadas").remove()
//        // Check that marko is connected to only one person
//        personSrv.get("marko").knownLevels must contain(exactly(1.0))
//      }

      "create initial values" in db.transaction { implicit graph ⇒
        personSrv.initSteps.name.toList must contain(exactly("marko", "vadas", "franck", "marc", "josh", "peter"))
      }
    }
  }
}
