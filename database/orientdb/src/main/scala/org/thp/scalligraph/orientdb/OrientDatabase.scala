package org.thp.scalligraph.orientdb

import java.io.InputStream
import java.util.{List ⇒ JList, Set ⇒ JSet}

import scala.collection.JavaConverters._
import scala.util.Try
import play.api.Configuration
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.{OClass, OSchema, OType}
import com.orientechnologies.orient.core.record.impl.ORecordBytes
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException
import gremlin.scala.{Element, Vertex, _}
import javax.inject.Singleton
import org.apache.tinkerpop.gremlin.orientdb.{OrientGraph, OrientGraphFactory}
import org.apache.tinkerpop.gremlin.structure.Graph
import org.thp.scalligraph.models.{IndexType, _}
import org.thp.scalligraph.{InternalError, Retry}

@Singleton
class OrientDatabase(graphFactory: OrientGraphFactory, maxRetryOnConflict: Int, override val chunkSize: Int) extends BaseDatabase {
  val attachmentVertexLabel  = "binaryData"
  val attachmentPropertyName = "binary"

  def this(url: String, maxRetryOnConflict: Int, chunkSize: Int) = this(new OrientGraphFactory(url), maxRetryOnConflict, chunkSize)

  def this(configuration: Configuration) =
    this(
      configuration.getOptional[String]("database.url").getOrElse(s"memory:test-${math.random}"),
      configuration.getOptional[Int]("database.maxRetryOnConflict").getOrElse(5),
      configuration.getOptional[Int]("db.chunkSize").getOrElse(32 * 1024)
    )

  def this() = this(Configuration.empty)

  override def noTransaction[A](body: Graph ⇒ A): A = body(graphFactory.getNoTx)

  override def transaction[A](body: Graph ⇒ A): A =
    Retry(maxRetryOnConflict, classOf[OConcurrentModificationException], classOf[ORecordDuplicatedException]) {
      val tx = graphFactory.getTx
      logger.debug(s"[$tx] Begin of transaction")
      try {
        val a = body(tx)
        tx.commit()
        a
      } catch {
        case e: Throwable ⇒
          Try(tx.rollback())
          throw e
      } finally {
        logger.debug(s"[$tx] End of transaction")
        tx.close()
      }
    }.fold[A](
      {
        case t: OConcurrentModificationException ⇒ throw new DatabaseException(cause = t)
        case t: ORecordDuplicatedException       ⇒ throw new DatabaseException(cause = t)
        case t                                   ⇒ throw t
      },
      a ⇒ a
    )

  private def getVariablesVertex(implicit graph: Graph): Option[Vertex] = graph.traversal().V().hasLabel("variables").headOption()

  override def version: Int = transaction { implicit graph ⇒
    getVariablesVertex.fold(0)(v ⇒ getSingleProperty(v, "version", UniMapping.intMapping))
  }

  override def setVersion(v: Int): Unit = transaction { implicit graph ⇒
    val variables = getVariablesVertex.getOrElse(graph.addVertex("variables"))
    setSingleProperty(variables, "version", v, UniMapping.intMapping)
  }

  private def createElementSchema(schema: OSchema, model: Model, superClassName: String, strict: Boolean): OClass = {
    val superClass = schema.getClass(superClassName)
    val clazz      = schema.createClass(model.label, superClass)
    model.fields.foreach {
      case (field, sm: SingleMapping[_, _]) ⇒
        clazz.createProperty(field, OType.getTypeByClass(sm.graphTypeClass)).setMandatory(strict).setNotNull(true).setReadonly(sm.isReadonly)
      case (field, om: OptionMapping[_, _]) ⇒
        clazz.createProperty(field, OType.getTypeByClass(om.graphTypeClass)).setMandatory(false).setNotNull(false).setReadonly(om.isReadonly)
      case (field, lm: ListMapping[_, _]) ⇒
        clazz
          .createProperty(field, OType.EMBEDDEDLIST, OType.getTypeByClass(lm.graphTypeClass))
          .setMandatory(false)
          .setNotNull(false)
          .setReadonly(lm.isReadonly)
      case (field, sm: SetMapping[_, _]) ⇒
        clazz
          .createProperty(field, OType.EMBEDDEDSET, OType.getTypeByClass(sm.graphTypeClass))
          .setMandatory(false)
          .setNotNull(false)
          .setReadonly(sm.isReadonly)
    }
    clazz.createProperty("_id", OType.STRING).setMandatory(strict).setNotNull(true).setReadonly(true)
    clazz.createProperty("_createdBy", OType.STRING).setMandatory(strict).setNotNull(true).setReadonly(true)
    clazz.createProperty("_createdAt", OType.DATETIME).setMandatory(strict).setNotNull(true).setReadonly(true)
    clazz.createProperty("_updatedBy", OType.STRING).setMandatory(false).setNotNull(false)
    clazz.createProperty("_updatedAt", OType.DATETIME).setMandatory(false).setNotNull(false)
    clazz.setStrictMode(strict)

    clazz.createIndex(s"${model.label}__id", INDEX_TYPE.UNIQUE, "_id")

    model.indexes.foreach {
      case (IndexType.unique, fields) ⇒
        clazz.createIndex(s"${model.label}_${fields.mkString("_")}", INDEX_TYPE.UNIQUE, fields: _*)
      case (IndexType.standard, fields) ⇒
        clazz.createIndex(s"${model.label}_${fields.mkString("_")}", INDEX_TYPE.DICTIONARY, fields: _*)
      case (IndexType.fulltext, fields) ⇒
        clazz.createIndex(s"${model.label}_${fields.mkString("_")}", INDEX_TYPE.FULLTEXT, fields: _*)
      case indexType ⇒ throw InternalError(s"Unrecognized index type: $indexType")
    }
    clazz
  }

  private def createEdgeProperties(schema: OSchema, model: EdgeModel[_, _], edgeClass: OClass): Unit =
    for {
      fromClass ← Option(schema.getClass(model.fromLabel))
      toClass   ← Option(schema.getClass(model.toLabel))
      className = edgeClass.getName
    } {
      fromClass.createProperty(s"out_$className", OType.LINKBAG, edgeClass)
      toClass.createProperty(s"in_$className", OType.LINKBAG, edgeClass)
      edgeClass.createProperty("in", OType.LINK, toClass)
      edgeClass.createProperty("out", OType.LINK, fromClass)
    }

  override def createSchema(models: Seq[Model]): Unit = {
    val schema = graphFactory.getNoTx.database().getMetadata.getSchema
    models.foreach {
      case model: VertexModel ⇒ createElementSchema(schema, model, OClass.VERTEX_CLASS_NAME, strict = false)
      case model: EdgeModel[_, _] ⇒
        val edgeClass = createElementSchema(schema, model, OClass.EDGE_CLASS_NAME, strict = false)
        createEdgeProperties(schema, model, edgeClass)
    }

    /* create the vertex for attachments */
    val superClass = schema.getClass(OClass.VERTEX_CLASS_NAME)
    val clazz      = schema.createClass(attachmentVertexLabel, superClass)
    clazz.createProperty(attachmentPropertyName, OType.LINKLIST)
    ()
  }

  override def drop(): Unit = graphFactory.drop()

  override def getListProperty[D, G](element: Element, key: String, mapping: ListMapping[D, G]): Seq[D] =
    element
      .value[JList[G]](key)
      .asScala
      .map(mapping.toDomain)

  override def getSetProperty[D, G](element: Element, key: String, mapping: SetMapping[D, G]): Set[D] =
    element
      .value[JSet[G]](key)
      .asScala
      .map(mapping.toDomain)
      .toSet

  override def setListProperty[D, G](element: Element, key: String, values: Seq[D], mapping: ListMapping[D, _]): Unit = {
    element.property(key, values.flatMap(mapping.toGraphOpt).asJava)
    ()
  }

  override def setSetProperty[D, G](element: Element, key: String, values: Set[D], mapping: SetMapping[D, _]): Unit = {
    element.property(key, values.flatMap(mapping.toGraphOpt).asJava)
    ()
  }

  override def loadBinary(id: String)(implicit graph: Graph): InputStream = loadBinary(graph.V().has(Key("_id") of id).head()) // check

  override def loadBinary(vertex: Vertex)(implicit graph: Graph): InputStream =
    new InputStream {
      private var recordIds                   = vertex.value[JList[OIdentifiable]]("binary").asScala.toList
      private var buffer: Option[Array[Byte]] = _
      private var index                       = 0
      private def getNextChunk(): Unit =
        recordIds match {
          case first :: tail ⇒
            recordIds = tail
            buffer = Some(first.getRecord[ORecordBytes].toStream)
            index = 0
          case _ ⇒ buffer = None
        }
      override def read(): Int =
        buffer match {
          case Some(b) if b.length > index ⇒
            val d = b(index)
            index += 1
            d.toInt & 0xff
          case None ⇒ -1
          case _ ⇒
            getNextChunk()
            read()
        }
    }

  override def saveBinary(is: InputStream)(implicit graph: Graph): Vertex = {
    val db = graph.asInstanceOf[OrientGraph].database()

    db.declareIntent(new OIntentMassiveInsert)
    val chunkIds = Iterator
      .continually {
        val chunk = new ORecordBytes
        val len   = chunk.fromInputStream(is, chunkSize)
        db.save[ORecordBytes](chunk)
        len → chunk.getIdentity.asInstanceOf[OIdentifiable]
      }
      .takeWhile(_._1 > 0)
      .map(_._2)
      .to[Seq]
    db.declareIntent(null)
    val v = graph.addVertex(attachmentVertexLabel)
    v.property(attachmentPropertyName, chunkIds.asJava)
    v
  }
}
