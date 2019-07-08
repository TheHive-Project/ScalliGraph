package org.thp.scalligraph.services

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.util.{Base64, UUID}

import scala.util.{Success, Try}

import play.api.{Configuration, Logger}

import gremlin.scala._
import javax.inject.{Inject, Singleton}
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileSystem => HDFileSystem, Path => HDPath}
import org.apache.hadoop.io.IOUtils
import org.thp.scalligraph.models.{Database, UniMapping, ValueMap}

trait StorageSrv {
  def loadBinary(id: String): InputStream
  def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Try[Unit]
  def saveBinary(id: String, data: Array[Byte])(implicit graph: Graph): Try[Unit] = saveBinary(id, new ByteArrayInputStream(data))
}

@Singleton
class LocalFileSystemStorageSrv(directory: Path) extends StorageSrv {
  if (!Files.exists(directory))
    Files.createDirectory(directory)

  @Inject()
  def this(configuration: Configuration) = this(Paths.get(configuration.get[String]("storage.localfs.location")))

  def loadBinary(id: String): InputStream =
    Files.newInputStream(directory.resolve(id))

  def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      Files.copy(is, directory.resolve(id))
      ()
    }.recover {
      case _: FileAlreadyExistsException => ()
    }
}

object HadoopStorageSrv {

  def loadConfiguration(conf: Configuration): HadoopConfig = {
    val hadoopConfig = new HadoopConfig()
    conf.entrySet.foreach {
      case (name, value) =>
        value.unwrapped() match {
          case s: String => hadoopConfig.set(name, s)
        }
    }
    hadoopConfig
  }
}

@Singleton
class HadoopStorageSrv(fs: HDFileSystem, location: HDPath) extends StorageSrv {

  @Inject()
  def this(configuration: Configuration) =
    this(
      HDFileSystem.get(
        URI.create(configuration.get[String]("storage.hdfs.root")),
        HadoopStorageSrv.loadConfiguration(configuration.get[Configuration]("storage.hdfs"))
      ),
      new HDPath(configuration.get[String]("storage.hdfs.location"))
    )

  override def loadBinary(id: String): InputStream =
    fs.open(new HDPath(id)).getWrappedStream

  override def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      val os = fs.create(new HDPath(location, id), false)
      IOUtils.copyBytes(is, os, 4096)
    }
}

@Singleton
class DatabaseStorageSrv(db: Database, chunkSize: Int) extends StorageSrv {

  val b64decoder: Base64.Decoder = Base64.getDecoder

  @Inject
  def this(db: Database, configuration: Configuration) = this(db, configuration.underlying.getBytes("storage.database.chunkSize").toInt)

  case class State(vertexId: String, buffer: Array[Byte]) {

    def next: Option[State] = db.noTransaction { implicit graph =>
      graph
        .V()
        .has(Key("_id") of vertexId)
        .out("nextChunk")
        .headOption()
        .map {
          case ValueMap(valueMap) =>
            State(valueMap.get[String]("_id"), b64decoder.decode(valueMap.get[String]("binary")))
        }
    }
  }

  object State {

    def apply(id: String): Option[State] = db.transaction { implicit graph =>
      graph
        .V()
        .has(Key("attachmentId") of id)
        .valueMap("_id", "binary")
        .headOption()
        .map {
          case ValueMap(valueMap) =>
            State(valueMap.get[String]("_id"), b64decoder.decode(valueMap.get[String]("binary")))
        }
    }
  }

  override def loadBinary(id: String): InputStream =
    new InputStream {
      var state = State(id)
      var index = 0

      override def read(): Int =
        state match {
          case Some(State(_, b)) if b.length > index =>
            val d = b(index)
            index += 1
            d.toInt & 0xff
          case None => -1
          case Some(s) =>
            state = s.next
            index = 0
            read()
        }
    }

  override def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Try[Unit] = {

    lazy val logger = Logger(getClass)
    def readNextChunk: String = {
      val buffer = new Array[Byte](chunkSize)
      val len    = is.read(buffer)
      logger.trace(s"$len bytes read")
      if (len == chunkSize)
        Base64.getEncoder.encodeToString(buffer)
      else if (len > 0)
        Base64.getEncoder.encodeToString(buffer.take(len))
      else
        ""
    }

    val chunks: Iterator[Vertex] = Iterator
      .continually(readNextChunk)
      .takeWhile(_.nonEmpty)
      .map { data =>
        val v = graph.addVertex("binary")
        db.setSingleProperty(v, "binary", data, UniMapping.stringMapping)
        db.setSingleProperty(v, "_id", UUID.randomUUID, db.idMapping)
        v
      }
    if (chunks.isEmpty) {
      logger.debug("Saving empty file")
      val v = graph.addVertex("binary")
      db.setSingleProperty(v, "binary", "", UniMapping.stringMapping)
      db.setSingleProperty(v, "attachmentId", id, UniMapping.stringMapping)
      db.setSingleProperty(v, "_id", UUID.randomUUID, db.idMapping)
    } else {
      val firstVertex = chunks.next
      db.setSingleProperty(firstVertex, "attachmentId", id, UniMapping.stringMapping)
      chunks.foldLeft(firstVertex) {
        case (previousVertex, currentVertex) =>
          previousVertex.addEdge("nextChunk", currentVertex)
          currentVertex
      }
    }
    Success(())
  }
}
