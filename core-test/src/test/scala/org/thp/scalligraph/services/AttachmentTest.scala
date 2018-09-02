package org.thp.scalligraph.services

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.controllers.FFile
import org.thp.scalligraph.models.DatabaseProviders
import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.{Configuration, Environment}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.tools.nsc.interpreter.InputStream

class AttachmentTest extends Specification {
  private val sys = ActorSystem("AttachmentTest")
  private val mat = ActorMaterializer()(sys)
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  def streamCompare(is1: InputStream, is2: InputStream): Boolean = {
    val n1 = is1.read()
    val n2 = is2.read()
    if (n1 == -1 || n2 == -1) n1 == n2
    else (n1 == n2) && streamCompare(is1, is2)
  }

  Fragments.foreach(DatabaseProviders.list) { dbProvider ⇒
    val db = dbProvider.get()
    db.createSchema(Nil)
    s"[${dbProvider.name}] attachment" should {

      "store binary data" in db.transaction { implicit graph ⇒
        val filePath      = Paths.get("build.sbt")
        val attachmentSrv = new AttachmentSrv(Seq("SHA1", "SHA-256"), db, global, mat)
        val file          = FFile("build.sbt", filePath, "text/plain")
        val attachment    = Await.result(attachmentSrv.save(file), 10.seconds)
        attachment.contentType must_=== "text/plain"
      }

      "read stored data" in db.transaction { implicit graph ⇒
        val filePath      = Paths.get("build.sbt")
        val attachmentSrv = new AttachmentSrv(Seq("SHA1", "SHA-256"), db, global, mat)
        val file          = FFile("build.sbt", filePath, "text/plain")
        val attachment    = Await.result(attachmentSrv.save(file), 10.seconds)
        val is1           = attachmentSrv.stream(attachment)
        val is2           = Files.newInputStream(filePath)
        try {
          streamCompare(is1, is2) must beTrue
        } finally {
          is1.close()
          is2.close()
        }
      }
    }
  }
}
