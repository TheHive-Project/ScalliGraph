package org.thp.scalligraph.services
import java.io.InputStream
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.{Base64, UUID}

import play.api.{Configuration, Logger}

import gremlin.scala._
import javax.inject.{Inject, Singleton}
import org.apache.hadoop.conf.{Configuration ⇒ HadoopConfig}
import org.apache.hadoop.fs.{FileSystem ⇒ HDFileSystem, Path ⇒ HDPath}
import org.apache.hadoop.io.IOUtils
import org.apache.tinkerpop.gremlin.structure.T
import org.thp.scalligraph.models.{Database, UniMapping}

trait StorageSrv {
  def loadBinary(id: String)(implicit graph: Graph): InputStream
  def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Vertex
}

@Singleton
class LocalFileSystemStorageSrv(directory: Path) extends StorageSrv {

  @Inject()
  def this(configuration: Configuration) = this(Paths.get(configuration.get[String]("storage.localfs.location")))

  def loadBinary(id: String)(implicit graph: Graph): InputStream =
    Files.newInputStream(directory.resolve(id))

  def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Vertex = {
    val id = UUID.randomUUID().toString
    Files.copy(is, directory.resolve(id))
    graph.addVertex(T.label, "binaryData", "_id", id)
  }
}

object HadoopStorageSrv {

  def loadConfiguration(conf: Configuration): HadoopConfig = {
    val hadoopConfig = new HadoopConfig()
    conf.entrySet.foreach {
      case (name, value) ⇒
        value.unwrapped() match {
          case s: String ⇒ hadoopConfig.set(name, s)
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

  override def loadBinary(id: String)(implicit graph: Graph): InputStream =
    fs.open(new HDPath(id)).getWrappedStream

  override def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Vertex = {
//    val id = UUID.randomUUID().toString
    val os = fs.create(new HDPath(location, id), false)
    IOUtils.copyBytes(is, os, 4096)
    graph.addVertex(T.label, "binaryData", "_id", id)
  }
}

@Singleton
class DatabaseStorageSrv(db: Database, chunkSize: Int) extends StorageSrv {

  @Inject
  def this(db: Database, configuration: Configuration) = this(db, configuration.underlying.getBytes("storage.database.chunkSize").toInt)

  override def loadBinary(id: String)(implicit graph: Graph): InputStream =
    new InputStream {
      var vertex: GremlinScala[Vertex] = graph.V().has(Key("_id") of id)
      var buffer: Option[Array[Byte]]  = vertex.clone.value[String]("binary").map(Base64.getDecoder.decode).headOption()
      var index                        = 0

      override def read(): Int =
        buffer match {
          case Some(b) if b.length > index ⇒
            val d = b(index)
            index += 1
            d.toInt & 0xff
          case None ⇒ -1
          case _ ⇒
            vertex = vertex.out("nextChunk")
            buffer = vertex.clone.value[String]("binary").map(Base64.getDecoder.decode).headOption()
            index = 0
            read()
        }
    }

  override def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Vertex = {

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
      .map { data ⇒
        val v = graph.addVertex("binary")
        db.setSingleProperty(v, "binary", data, UniMapping.stringMapping)
        db.setSingleProperty(v, "_id", UUID.randomUUID, db.idMapping)
        v
      }
    if (chunks.isEmpty) {
      logger.debug("Saving empty file")
      val v = graph.addVertex("binary")
      db.setSingleProperty(v, "binary", "", UniMapping.stringMapping)
      db.setSingleProperty(v, "_id", UUID.randomUUID, db.idMapping)
      v
    } else {
      val firstVertex = chunks.next
      chunks.foldLeft(firstVertex) {
        case (previousVertex, currentVertex) ⇒
          previousVertex.addEdge("nextChunk", currentVertex)
          currentVertex
      }
      firstVertex
    }
  }
}
