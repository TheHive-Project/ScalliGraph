package org.thp.scalligraph.services

import java.io.InputStream
import java.nio.file.Files

import akka.stream.scaladsl.{Source, StreamConverters}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import gremlin.scala.{Graph, Vertex}
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.controllers.{Attachment, FFile}
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.{Hash, Hasher}
import play.api.Configuration
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}

case class FAttachment(name: String, size: Long, contentType: String, hashes: Map[String, Hash], vertex: Vertex) extends Attachment {
  override def toJson: JsObject = Json.obj(
    "name"        → name,
    "size"        → size,
    "contentType" → contentType,
    "hashes"      → hashes
  )
}

@Singleton
class AttachmentSrv(hashList: Seq[String], db: Database, implicit val ec: ExecutionContext, implicit val mat: Materializer) {

  @Inject() def this(configuration: Configuration, database: Database, ec: ExecutionContext, mat: Materializer) =
    this(
      configuration.get[Seq[String]]("datastore.hash"),
      database: Database,
      ec,
      mat
    )

  val hashers = Hasher(hashList: _*)

  def save(f: FFile)(implicit graph: Graph): Future[FAttachment] = {
    val hashes = hashers.fromPath(f.filepath).map { hs ⇒
      (hashList zip hs).toMap
    }
    val is = Files.newInputStream(f.filepath)
    val v  = db.saveBinary(is)
    is.close()
    hashes.map(FAttachment(f.filename, Files.size(f.filepath), f.contentType, _, v))
  }

  def source(attachment: FAttachment)(implicit graph: Graph): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() ⇒ stream(attachment))

  def stream(attachment: FAttachment)(implicit graph: Graph): InputStream = db.loadBinary(attachment.vertex)
}
