package org.thp.scalligraph.services

import gremlin.scala.Graph
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.AppBuilder
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import org.thp.scalligraph.models._
import org.thp.scalligraph.steps.StepsOps._
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import scala.util.{Success, Try}

class DedupTest extends PlaySpecification {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  implicit val authContext: AuthContext = AuthContextImpl("me", "", "", "", Set.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] dedup" should {
      "copy edges vertex" in {
        val app: AppBuilder = new AppBuilder()
          .bindToProvider(dbProvider)
        implicit val db: Database = app[Database]
        val personSrv             = app[PersonSrv]
        val softwareSrv           = app[SoftwareSrv]
        val createdSrv            = new EdgeSrv[Created, Person, Software]
        ModernDatabaseBuilder.build(app[ModernSchema])(app[Database], authContext)
        val newLop = db.tryTransaction { implicit graph =>
          val lop   = softwareSrv.create(Software("lop", "asm")).get
          val vadas = personSrv.get("vadas").head()
          createdSrv
            .create(Created(0.1), vadas, lop)
            .map(_ => lop)
        }.get

        val dedupOps: DedupOps[Software] = new DedupOps[Software] {
          override val db: Database         = app[Database]
          override val service: SoftwareSrv = app[SoftwareSrv]

          override def resolve(entities: List[Software with Entity])(implicit graph: Graph): Try[Unit] = Success(())
        }
        val duplicates = dedupOps.getDuplicates("name")
        duplicates must have size 1
        duplicates.head.map(s => s.name -> s.lang) must contain(exactly("lop" -> "java", "lop" -> "asm"))
        db.tryTransaction { implicit graph =>
          duplicates.head.sorted(dedupOps.createdFirst.reverse) match {
            case lastCreated :: others =>
              println(s"copy edge from ${others.map(v => s"$v(${v._id})").mkString(", ")} to $lastCreated(${lastCreated._id})")
              others.foreach(dedupOps.copyEdge(_, lastCreated))
            case _ => failure("should never append")
          }
          Success(())
        }

        db.roTransaction { implicit graph =>
          softwareSrv.get(newLop).createdBy.toList.map(_.name) must contain(exactly("vadas", "marko", "josh", "peter"))
        }
      }
    }
  }
}
