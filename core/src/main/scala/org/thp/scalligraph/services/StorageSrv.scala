package org.thp.scalligraph.services

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.file._
import java.util.Base64

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Success, Try}

import play.api.{Configuration, Logger}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import gremlin.scala._
import javax.inject.{Inject, Singleton}
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileAlreadyExistsException => HadoopFileAlreadyExistsException, FileSystem => HDFileSystem, Path => HDPath}
import org.apache.hadoop.io.IOUtils
import org.thp.scalligraph.auth.UserSrv
import org.thp.scalligraph.models.{Binary, BinaryLink, Database, Entity}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps

trait StorageSrv {
  def loadBinary(folder: String, id: String): InputStream
  def source(folder: String, id: String): Source[ByteString, _] = StreamConverters.fromInputStream(() => loadBinary(folder, id))
  def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit]

  def saveBinary(folder: String, id: String, data: Array[Byte])(implicit graph: Graph): Try[Unit] =
    saveBinary(folder: String, id, new ByteArrayInputStream(data))

  def saveBinary(folder: String, id: String, data: Source[ByteString, NotUsed])(implicit graph: Graph, mat: Materializer): Try[Unit] =
    saveBinary(folder, id, data.runWith(StreamConverters.asInputStream(5.minutes)))
  def exists(folder: String, id: String): Boolean
}

@Singleton
class LocalFileSystemStorageSrv(directory: Path) extends StorageSrv {
  if (!Files.exists(directory))
    Files.createDirectory(directory)

  @Inject()
  def this(configuration: Configuration) = this(Paths.get(configuration.get[String]("storage.localfs.location")))

  override def loadBinary(folder: String, id: String): InputStream =
    Files.newInputStream(directory.resolve(folder).resolve(id))

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      Files.copy(is, directory.resolve(folder).resolve(id))
      ()
    }.recover {
      case _: NoSuchFileException =>
        Files.createDirectories(directory.resolve(folder))
        Files.copy(is, directory.resolve(folder).resolve(id))
        ()
      case _: FileAlreadyExistsException => ()
    }

  override def exists(folder: String, id: String): Boolean = Files.exists(directory.resolve(folder).resolve(id))
}

object HadoopStorageSrv {

  def loadConfiguration(conf: Configuration): HadoopConfig = {
    val hadoopConfig = new HadoopConfig()
    conf.entrySet.foreach {
      case ("username", username) =>
        System.setProperty("HADOOP_USER_NAME", username.unwrapped().toString)
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

  override def loadBinary(folder: String, id: String): InputStream =
    fs.open(new HDPath(new HDPath(location, folder), id)).getWrappedStream

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      val os = fs.create(new HDPath(new HDPath(location, folder), id), false)
      IOUtils.copyBytes(is, os, 4096)
      os.close()
    }.recover {
      case _: HadoopFileAlreadyExistsException => ()
    }

  override def exists(folder: String, id: String): Boolean = fs.exists(new HDPath(new HDPath(location, folder), id))
}

@Singleton
class DatabaseStorageSrv(chunkSize: Int, userSrv: UserSrv, implicit val db: Database) extends StorageSrv {

  val b64decoder: Base64.Decoder = Base64.getDecoder
  val binaryLinkSrv              = new EdgeSrv[BinaryLink, Binary, Binary]()

  @Inject
  def this(configuration: Configuration, userSrv: UserSrv, db: Database) =
    this(configuration.underlying.getBytes("storage.database.chunkSize").toInt, userSrv, db)

  case class State(binary: Binary with Entity) {

    def next: Option[State] = db.roTransaction { implicit graph =>
      new VertexSteps[Binary](
        graph
          .V(binary._id)
          .out("nextChunk")
      ).headOption()
        .map(State.apply)
    }
    def data: Array[Byte] = binary.data
  }

  object State {

    def apply(folder: String, id: String): Option[State] = db.roTransaction { implicit graph =>
      new VertexSteps[Binary](graph.V)
        .has("folder", folder)
        .has("attachmentId", id)
        .headOption()
        .map(State.apply)
    }
  }

  override def loadBinary(folder: String, id: String): InputStream =
    new InputStream {
      var state: Option[State] = State(folder, id)
      var index                = 0

      @scala.annotation.tailrec
      override def read(): Int =
        state match {
          case Some(state) if state.data.length > index =>
            val data = state.data(index)
            index += 1
            data.toInt & 0xff
          case None => -1
          case Some(s) =>
            state = s.next
            index = 0
            read()
        }
    }

  override def exists(folder: String, id: String): Boolean = db.roTransaction { implicit graph =>
    new VertexSteps[Binary](graph.V)
      .has("folder", folder)
      .has("attachmentId", id)
      .exists()
  }

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] = {

    lazy val logger: Logger = Logger(getClass)

    def readNextChunk: Array[Byte] = {
      val buffer = new Array[Byte](chunkSize)
      val len    = is.read(buffer)
      logger.trace(s"$len bytes read")
      if (len == chunkSize) buffer
      else if (len > 0) buffer.take(len)
      else Array.emptyByteArray
    }

    if (exists(folder, id)) Success(())
    else {
      val chunks: Iterator[Binary with Entity] = Iterator
        .continually(readNextChunk)
        .takeWhile(_.nonEmpty)
        .map { data =>
          db.createVertex[Binary](graph, userSrv.getSystemAuthContext, db.binaryModel, Binary(Some(id), Some(folder), data))
        }
      if (chunks.isEmpty) {
        logger.debug("Saving empty file")
        db.createVertex(graph, userSrv.getSystemAuthContext, db.binaryModel, Binary(None, None, Array.emptyByteArray))
      } else {
        logger.debug(s"Save file $folder/$id")
        chunks.foldLeft(chunks.next) {
          case (previousVertex, currentVertex) =>
            db.createEdge[BinaryLink, Binary, Binary](
              graph,
              userSrv.getSystemAuthContext,
              db.binaryLinkModel,
              BinaryLink(),
              previousVertex,
              currentVertex
            )
            currentVertex
        }
      }
      Success(())
    }
  }
}

@Singleton
class S3StorageSrv @Inject() (configuration: Configuration, implicit val ec: ExecutionContext, implicit val mat: Materializer) extends StorageSrv {
  val bucketName: String           = configuration.get[String]("storage.s3.bucket")
  val readTimeout: FiniteDuration  = configuration.get[FiniteDuration]("storage.s3.readTimeout")
  val writeTimeout: FiniteDuration = configuration.get[FiniteDuration]("storage.s3.writeTimeout")
  val chunkSize: Int               = configuration.get[Int]("storage.s3.chunkSize")

  override def loadBinary(folder: String, id: String): InputStream =
    S3.download(bucketName, s"$folder/$id")
      .flatMapConcat(_.get._1)
      .runWith(
        StreamConverters.asInputStream(readTimeout)
      )

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      Await.ready(StreamConverters.fromInputStream(() => is, chunkSize).runWith(S3.multipartUpload(bucketName, s"$folder/$id")), writeTimeout)
      ()
    }

  override def exists(folder: String, id: String): Boolean =
    Try {
      Await.result(S3.getObjectMetadata(bucketName, s"$folder/$id").runWith(Sink.head).map(_.isDefined), readTimeout)
    }.getOrElse(false)
}
