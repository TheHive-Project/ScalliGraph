package org.thp.scalligraph.services

import org.specs2.specification.core.Fragments
import org.thp.scalligraph.EntityName
import org.thp.scalligraph.auth.{AuthContext, AuthContextImpl}
import org.thp.scalligraph.models.{ModernOps, _}
import org.thp.scalligraph.traversal.Graph
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.test.PlaySpecification
import play.api.{Configuration, Environment}

import scala.util.{Success, Try}

class IntegrityCheckTest extends PlaySpecification with ModernOps {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)
  implicit val authContext: AuthContext = AuthContextImpl("me", "", EntityName(""), "", Set.empty)

  Fragments.foreach(new DatabaseProviders().list) { dbProvider =>
    s"[${dbProvider.name}] integrity check" should {
      "copy edges vertex" in {
        implicit val database: Database = dbProvider.get
        val personSrv                   = new PersonSrv
        val softwareSrv                 = new SoftwareSrv
        val createdSrv                  = new EdgeSrv[Created, Person, Software]
        ModernDatabaseBuilder.build(database)(authContext)
        val newLop = database.tryTransaction { implicit graph =>
          val lop   = softwareSrv.create(Software("lop", "asm")).get
          val vadas = personSrv.getByName("vadas").head
          createdSrv
            .create(Created(0.1), vadas, lop)
            .map(_ => lop)
        }.get

        val integrityCheckOps: IntegrityCheckOps[Software] = new IntegrityCheckOps[Software] {
          override val db: Database         = database
          override val service: SoftwareSrv = softwareSrv

          override def resolve(entities: Seq[Software with Entity])(implicit graph: Graph): Try[Unit] = Success(())

          override def globalCheck(): Map[String, Long] = Map.empty
        }
        val duplicates = integrityCheckOps.getDuplicates(Seq("name"))
        duplicates must have size 1
        duplicates.head.map(s => s.name -> s.lang) must contain(exactly("lop" -> "java", "lop" -> "asm"))
        database.tryTransaction { implicit graph =>
          integrityCheckOps.lastCreatedEntity(duplicates.head).foreach {
            case (lastCreated, others) =>
              println(s"copy edge from ${others.map(v => s"$v(${v._id})").mkString(", ")} to $lastCreated(${lastCreated._id})")
              others.foreach(integrityCheckOps.copyEdge(_, lastCreated))
          }
          Success(())
        }

        database.roTransaction { implicit graph =>
          softwareSrv.get(newLop).createdBy.toSeq.map(_.name) must contain(exactly("vadas", "marko", "josh", "peter"))
        }
      }
    }
  }
}
