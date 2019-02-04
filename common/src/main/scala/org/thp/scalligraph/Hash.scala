package org.thp.scalligraph

import java.io.InputStream
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import scala.concurrent.{ExecutionContext, Future}

import play.api.libs.json.{Format, JsString, Reads, Writes}

import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString

// TODO use play.api.libs.Codecs

case class Hasher(algorithms: String*) {

  val bufferSize = 4096
  def fromPath(path: Path): Seq[Hash] =
    fromInputStream(Files.newInputStream(path))

  def fromInputStream(is: InputStream): Seq[Hash] = {
    val mds = algorithms.map(algo ⇒ MessageDigest.getInstance(algo))
    def readNextBuffer = {
      val buffer = Array.ofDim[Byte](bufferSize)
      val len    = is.read(buffer)
      if (len == bufferSize) buffer else buffer.take(len)
    }

    Iterator
      .continually(readNextBuffer)
      .takeWhile(_.nonEmpty)
      .foreach(buffer ⇒ mds.foreach(md ⇒ md.update(buffer)))
    mds.map(md ⇒ Hash(md.digest()))
  }

  def fromString(data: String): Seq[Hash] = {
    val mds = algorithms.map(algo ⇒ MessageDigest.getInstance(algo))
    mds.map(md ⇒ Hash(md.digest(data.getBytes(Charset.forName("UTF8")))))
  }
}

class MultiHash(algorithms: String)(implicit mat: Materializer, ec: ExecutionContext) {
  private val md = MessageDigest.getInstance(algorithms)
  def addValue(value: String): Unit = {
    md.update(0.asInstanceOf[Byte])
    md.update(value.getBytes)
  }
  def addFile(filename: String): Future[Unit] = addFile(Paths.get(filename))
  def addFile(file: Path): Future[Unit] = {
    md.update(0.asInstanceOf[Byte])
    FileIO
      .fromPath(file)
      .runForeach(bs ⇒ md.update(bs.toByteBuffer))
      .map(_ ⇒ ())
  }
  def addSource(source: Source[ByteString, _]): Future[Unit] =
    source
      .runForeach { bs ⇒
        md.update(bs.toByteBuffer)
      }
      .map(_ ⇒ ())
  def digest: Hash = Hash(md.digest())
}

case class Hash(data: Array[Byte]) {
  override def toString: String = data.map(b ⇒ f"$b%02x").mkString

  override def equals(obj: scala.Any): Boolean = obj match {
    case Hash(d) ⇒ d.deep == data.deep
    case _       ⇒ false
  }
}
object Hash {
  def apply(s: String): Hash = Hash {
    s.grouped(2)
      .map { cc ⇒
        (Character.digit(cc(0), 16) << 4 | Character.digit(cc(1), 16)).toByte
      }
      .toArray
  }

  val hashReads                         = Reads(json ⇒ json.validate[String].map(h ⇒ Hash(h)))
  val hashWrites: Writes[Hash]          = Writes[Hash](h ⇒ JsString(h.toString()))
  implicit val hashFormat: Format[Hash] = Format(hashReads, hashWrites)
}
