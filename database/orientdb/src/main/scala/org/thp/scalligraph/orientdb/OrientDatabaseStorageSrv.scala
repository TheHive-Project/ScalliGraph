package org.thp.scalligraph.orientdb
import java.io.InputStream
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

import play.api.Configuration

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.record.impl.ORecordBytes
import gremlin.scala.{Graph, Key, _}
import javax.inject.{Inject, Singleton}
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import org.thp.scalligraph.services.StorageSrv

@Singleton
class OrientDatabaseStorageSrv(db: OrientDatabase, chunkSize: Int) extends StorageSrv {

  @Inject
  def this(db: OrientDatabase, configuration: Configuration) = this(db, configuration.underlying.getBytes("storage.database.chunkSize").toInt)

  override def loadBinary(id: String)(implicit graph: Graph): InputStream =
    new InputStream {
      private val vertex                      = graph.V().has(Key("_id") of id).head()
      private var recordIds                   = vertex.value[JList[OIdentifiable]]("binary").asScala.toList
      private var buffer: Option[Array[Byte]] = _
      private var index                       = 0
      private def getNextChunk(): Unit =
        recordIds match {
          case first :: tail =>
            recordIds = tail
            buffer = Some(first.getRecord[ORecordBytes].toStream)
            index = 0
          case _ => buffer = None
        }
      override def read(): Int =
        buffer match {
          case Some(b) if b.length > index =>
            val d = b(index)
            index += 1
            d.toInt & 0xff
          case None => -1
          case _ =>
            getNextChunk()
            read()
        }
    }

  override def saveBinary(id: String, is: InputStream)(implicit graph: Graph): Try[Unit] = {
    val odb = graph.asInstanceOf[OrientGraph].database()

    odb.declareIntent(new OIntentMassiveInsert)
    val chunkIds = Iterator
      .continually {
        val chunk = new ORecordBytes
        val len   = chunk.fromInputStream(is, chunkSize)
        odb.save[ORecordBytes](chunk)
        len -> chunk.getIdentity.asInstanceOf[OIdentifiable]
      }
      .takeWhile(_._1 > 0)
      .map(_._2)
      .toSeq
    odb.declareIntent(null)
    val v = graph.addVertex(db.attachmentVertexLabel)
    v.property("_id", id)
    v.property(db.attachmentPropertyName, chunkIds.asJava)
    Success(())
  }
}
