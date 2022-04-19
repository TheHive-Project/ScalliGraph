package org.thp.scalligraph.services

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.{S3Attributes, S3Settings}
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileAlreadyExistsException => HadoopFileAlreadyExistsException, FileSystem => HDFileSystem, Path => HDPath}
import org.apache.hadoop.io.IOUtils
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.UserSrv
import org.thp.scalligraph.models._
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Graph, Traversal}
import play.api.{Configuration, Logger}

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.file._
import java.util.Base64
import javax.inject.{Inject, Singleton}
import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Success, Try}

trait StorageSrv {
  def loadBinary(folder: String, id: String): InputStream
  def source(folder: String, id: String): Source[ByteString, _] = StreamConverters.fromInputStream(() => loadBinary(folder, id))
  def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit]

  def saveBinary(folder: String, id: String, data: Array[Byte])(implicit graph: Graph): Try[Unit] =
    saveBinary(folder: String, id, new ByteArrayInputStream(data))

  def saveBinary(folder: String, id: String, data: Source[ByteString, NotUsed])(implicit graph: Graph, mat: Materializer): Try[Unit] =
    saveBinary(folder, id, data.runWith(StreamConverters.asInputStream(5.minutes)))
  def exists(folder: String, id: String): Boolean

  def delete(folder: String, id: String)(implicit graph: Graph): Try[Unit]

  def getSize(folder: String, id: String)(implicit graph: Graph): Try[Long]
}

@Singleton
class LocalFileSystemStorageSrv(directory: Path) extends StorageSrv {
  if (!Files.exists(directory))
    Files.createDirectory(directory)

  @Inject()
  def this(configuration: Configuration) = this(Paths.get(configuration.get[String]("storage.localfs.location")))

  override def loadBinary(folder: String, id: String): InputStream =
    Files.newInputStream(directory.resolve(folder).resolve(id))

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] = {
    val result = Try {
      val f = directory.resolve(folder).resolve(id)
      if (!Files.exists(f))
        Files.copy(is, directory.resolve(folder).resolve(id))
      ()
    }.recover {
      case _: NoSuchFileException =>
        Files.createDirectories(directory.resolve(folder))
        Files.copy(is, directory.resolve(folder).resolve(id))
        ()
      case _: FileAlreadyExistsException => ()
    }
    is.close()
    result
  }

  override def exists(folder: String, id: String): Boolean = Files.exists(directory.resolve(folder).resolve(id))

  override def delete(folder: String, id: String)(implicit graph: Graph): Try[Unit] = Try(Files.delete(directory.resolve(folder).resolve(id)))

  override def getSize(folder: String, id: String)(implicit graph: Graph): Try[Long] = Try(Files.size(directory.resolve(folder).resolve(id)))
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

  override def delete(folder: String, id: String)(implicit graph: Graph): Try[Unit] =
    Try {
      fs.delete(new HDPath(new HDPath(location, folder), id), false)
      ()
    }

  override def getSize(folder: String, id: String)(implicit graph: Graph): Try[Long] =
    Try(fs.getFileStatus(new HDPath(new HDPath(location, folder), id)).getLen)
}

@Singleton
class DatabaseStorageSrv(chunkSize: Int, userSrv: UserSrv, implicit val db: Database) extends VertexSrv[Binary]()(Binary.model) with StorageSrv {

  val b64decoder: Base64.Decoder                       = Base64.getDecoder
  implicit val binaryLinkModel: Model.Edge[BinaryLink] = BinaryLink.model
  val binaryLinkSrv                                    = new EdgeSrv[BinaryLink, Binary, Binary]

  @Inject
  def this(configuration: Configuration, userSrv: UserSrv, db: Database) =
    this(configuration.underlying.getBytes("storage.database.chunkSize").toInt, userSrv, db)

  def get(folder: String, attachmentId: String)(implicit graph: Graph): Traversal.V[Binary] =
    startTraversal.unsafeHas("folder", folder).unsafeHas("attachmentId", attachmentId) // "has" macro can't be used because it is in the same project

  case class State(binary: Binary with Entity) {

    def next: Option[State] =
      db.roTransaction { implicit graph =>
        get(binary)
          .out("nextChunk")
          .v[Binary]
          .headOption
          .map(State.apply)
      }
    def data: Array[Byte] = binary.data
  }

  object State {

    def apply(folder: String, id: String): Option[State] =
      db.roTransaction { implicit graph =>
        get(folder, id)
          .headOption
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

  override def exists(folder: String, id: String): Boolean =
    db.roTransaction { implicit graph =>
      get(folder, id).exists
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

  override def delete(folder: String, id: String)(implicit graph: Graph): Try[Unit] = ???

  override def getSize(folder: String, id: String)(implicit graph: Graph): Try[Long] = ???
}

@Singleton
class S3StorageSrv @Inject() (configuration: Configuration, system: ActorSystem, implicit val ec: ExecutionContext, implicit val mat: Materializer)
    extends StorageSrv {
  private val logger                       = Logger(getClass)
  private val bucketName: String           = configuration.get[String]("storage.s3.bucket")
  private val readTimeout: FiniteDuration  = configuration.get[FiniteDuration]("storage.s3.readTimeout")
  private val writeTimeout: FiniteDuration = configuration.get[FiniteDuration]("storage.s3.writeTimeout")
  private val chunkSize: Int               = configuration.underlying.getBytes("storage.s3.chunkSize").toInt
  private val baseFolder: String           = configuration.getOptional[String]("storage.s3.folder").getOrElse("")

  private val settings: S3Settings = {
    val defaultConfig         = system.settings.config.getConfig(S3Settings.ConfigPath)
    val deprecatedEndpointKey = "storage.s3.endpoint"
    val endpointConfig = configuration
      .getOptional[String](deprecatedEndpointKey)
      .fold(ConfigFactory.empty()) { endpoint =>
        val origin = configuration.underlying.getValue(deprecatedEndpointKey).origin
        logger.warn(s"${origin.description}: $deprecatedEndpointKey is deprecated, use storage.s3.endpoint-url instead")
        ConfigFactory.parseMap(Map("endpoint-url" -> endpoint).asJava)
      }
    val deprecatedRegionKey = "storage.s3.region"
    val regionConfig = configuration
      .getOptional[String](deprecatedRegionKey)
      .fold(ConfigFactory.empty()) { region =>
        val origin = configuration.underlying.getValue(deprecatedRegionKey).origin
        logger.warn(
          s"${origin.description}: $deprecatedRegionKey is deprecated, set storage.s3.aws.region.provider to `static` and use storage.s3.aws.region.default-region instead"
        )
        ConfigFactory.parseMap(Map("aws.region.provider" -> "static", "aws.region.default-region" -> region).asJava)
      }
    val deprecatedAccessKey = "storage.s3.accessKey"
    val accessKeyConfig = configuration
      .getOptional[String](deprecatedAccessKey)
      .fold(ConfigFactory.empty()) { accessKey =>
        val origin = configuration.underlying.getValue(deprecatedAccessKey).origin
        logger.warn(
          s"${origin.description}: $deprecatedAccessKey is deprecated, set storage.s3.aws.credentials.provider to `static` and use storage.s3.aws.credentials.access-key-id instead"
        )
        ConfigFactory.parseMap(Map("aws.credentials.provider" -> "static", "aws.credentials.access-key-id" -> accessKey).asJava)
      }
    val deprecatedSecretKey = "storage.s3.secretKey"
    val secretKeyConfig = configuration
      .getOptional[String](deprecatedSecretKey)
      .fold(ConfigFactory.empty()) { secretKey =>
        val origin = configuration.underlying.getValue(deprecatedSecretKey).origin
        logger.warn(
          s"${origin.description}: $deprecatedSecretKey is deprecated, set storage.s3.aws.credentials.provider to `static` and use storage.s3.aws.credentials.secret-access-key instead"
        )
        ConfigFactory.parseMap(Map("aws.credentials.provider" -> "static", "aws.credentials.secret-access-key" -> secretKey).asJava)
      }

    S3Settings {
      configuration
        .get[Config]("storage.s3")
        .withFallback(endpointConfig)
        .withFallback(regionConfig)
        .withFallback(accessKeyConfig)
        .withFallback(secretKeyConfig)
        .withFallback(defaultConfig)
    }
  }

  override def source(folder: String, id: String): Source[ByteString, _] = {
    val filename = s"$baseFolder/$folder/$id"
    S3.download(bucketName, filename)
      .withAttributes(S3Attributes.settings(settings))
      .flatMapConcat(_.getOrElse(throw new NoSuchFileException(filename))._1)
  }

  override def loadBinary(folder: String, id: String): InputStream =
    source(folder, id)
      .runWith(
        StreamConverters.asInputStream(readTimeout)
      )

  override def saveBinary(folder: String, id: String, is: InputStream)(implicit graph: Graph): Try[Unit] =
    Try {
      Await.result(
        StreamConverters
          .fromInputStream(() => is, chunkSize)
          .runWith(S3.multipartUpload(bucketName, s"$baseFolder/$folder/$id").withAttributes(S3Attributes.settings(settings))),
        writeTimeout
      )
      ()
    }

  override def exists(folder: String, id: String): Boolean =
    Try {
      Await.result(
        S3.getObjectMetadata(bucketName, s"$baseFolder/$folder/$id")
          .withAttributes(S3Attributes.settings(settings))
          .runWith(Sink.head)
          .map(_.isDefined),
        readTimeout
      )
    }.getOrElse(false)

  override def delete(folder: String, id: String)(implicit graph: Graph): Try[Unit] =
    Try {
      Await.ready(
        S3.deleteObject(bucketName, s"$baseFolder/$folder/$id")
          .withAttributes(S3Attributes.settings(settings))
          .runWith(Sink.ignore),
        readTimeout
      )
      ()
    }

  override def getSize(folder: String, id: String)(implicit graph: Graph): Try[Long] =
    Try {
      Await
        .result(
          S3.getObjectMetadata(bucketName, s"$baseFolder/$folder/$id")
            .withAttributes(S3Attributes.settings(settings))
            .runWith(Sink.head),
          readTimeout
        )
        .fold(throw NotFoundError(s"Attachment $folder/$id not found"))(_.contentLength)
    }
}
