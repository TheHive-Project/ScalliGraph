package org.thp.scalligraph.services

import java.nio.file.{Files, Paths}

import scala.tools.nsc.interpreter.InputStream

import play.api.libs.logback.LogbackLoggerConfigurator
import play.api.{Configuration, Environment}

import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments
import org.thp.scalligraph.models.DatabaseProviders
import org.thp.scalligraph.orientdb.{OrientDatabase, OrientDatabaseStorageSrv}

class AttachmentTest extends Specification {
  (new LogbackLoggerConfigurator).configure(Environment.simple(), Configuration.empty, Map.empty)

  def streamCompare(is1: InputStream, is2: InputStream): Boolean = {
    val n1 = is1.read()
    val n2 = is2.read()
    if (n1 == -1 || n2 == -1) n1 == n2
    else (n1 == n2) && streamCompare(is1, is2)
  }

  val dbProviders = new DatabaseProviders()
  val dbProvStorageSrv = dbProviders.list.map {
    case db if db.name == "orientdb" ⇒ db → new OrientDatabaseStorageSrv(db.get().asInstanceOf[OrientDatabase], 32 * 1024)
    case db                          ⇒ db → new DatabaseStorageSrv(db.get(), 32 * 1024)
  } :+ dbProviders.janus → new LocalFileSystemStorageSrv(Paths.get("target/AttachmentTest"))

  Fragments.foreach(dbProvStorageSrv) {
    case (dbProvider, storageSrv) ⇒
      val db = dbProvider.get()
      db.createSchema(Nil)
      s"[${dbProvider.name}] attachment" should {

        "save and read stored data" in db.transaction { implicit graph ⇒
          val filePath = Paths.get("../build.sbt")
          val is       = Files.newInputStream(filePath)
          storageSrv.saveBinary("build.sbt-custom-id", is)
          is.close()

          val is1 = storageSrv.loadBinary("build.sbt-custom-id")
          val is2 = Files.newInputStream(filePath)
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
