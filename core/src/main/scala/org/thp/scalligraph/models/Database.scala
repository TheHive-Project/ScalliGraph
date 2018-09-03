package org.thp.scalligraph.models

import java.io.InputStream
import java.lang.reflect.Modifier
import java.util.{Base64, Date, UUID}

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe ⇒ ru}
import scala.reflect.{classTag, ClassTag}

import play.api.Logger

import gremlin.scala.{Graph, _}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.UpdateOps
import org.thp.scalligraph.services.{EdgeSrv, VertexSrv}
import org.thp.scalligraph.{FPath, InternalError}

trait Database {
  lazy val logger = Logger("org.thp.scalligraph.models.Database")

  val idMapping: SingleMapping[UUID, String]          = SingleMapping[UUID, String](classOf[String], uuid ⇒ Some(uuid.toString), UUID.fromString)
  val createdAtMapping: SingleMapping[Date, Date]     = UniMapping.dateMapping
  val createdByMapping: SingleMapping[String, String] = UniMapping.stringMapping
  val updatedAtMapping: OptionMapping[Date, Date]     = UniMapping.dateMapping.optional
  val updatedByMapping: OptionMapping[String, String] = UniMapping.stringMapping.optional

  def noTransaction[A](body: Graph ⇒ A): A
  def transaction[A](body: Graph ⇒ A): A

  def getModel[E <: Product: ru.TypeTag]: Model.Base[E] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val companionMirror =
      rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule)
    companionMirror.instance match {
      case hm: HasModel[_] ⇒ hm.model.asInstanceOf[Model.Base[E]]
      case _               ⇒ throw InternalError(s"Class ${companionMirror.symbol} is not a model")
    }
  }

  def getVertexModel[E <: Product: ru.TypeTag]: Model.Vertex[E] = {
    val rm              = ru.runtimeMirror(getClass.getClassLoader)
    val classMirror     = rm.reflectClass(ru.typeOf[E].typeSymbol.asClass)
    val companionSymbol = classMirror.symbol.companion
    val companionMirror = rm.reflectModule(companionSymbol.asModule)
    companionMirror.instance match {
      case hm: HasVertexModel[_] ⇒ hm.model.asInstanceOf[Model.Vertex[E]]
      case _                     ⇒ throw InternalError(s"Class ${companionMirror.symbol} is not a vertex model")
    }
  }

  def getEdgeModel[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product]: Model.Edge[E, FROM, TO] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val companionMirror =
      rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule)
    companionMirror.instance match {
      case hm: HasEdgeModel[_, _, _] ⇒ hm.model.asInstanceOf[Model.Edge[E, FROM, TO]]
      case _                         ⇒ throw InternalError(s"Class ${companionMirror.symbol} is not an edge model")
    }
  }

  def createSchemaFrom(schemaObject: Any)(implicit authContext: AuthContext): Unit = {
    def extract[F: ClassTag](o: Any): Seq[F] =
      o.getClass.getMethods.collect {
        case field
            if !Modifier.isPrivate(field.getModifiers) &&
              field.getParameterCount == 0 &&
              classTag[F].runtimeClass.isAssignableFrom(field.getReturnType) ⇒
          field.invoke(o).asInstanceOf[F]
      }.toSeq

    val models     = extract[Model](schemaObject)
    val vertexSrvs = extract[VertexSrv[_]](schemaObject)
    val edgeSrvs   = extract[EdgeSrv[_, _, _]](schemaObject)
    createSchema(models, vertexSrvs, edgeSrvs)
  }

  def createSchema(model: Model, models: Model*): Unit = createSchema(model +: models)

  def createSchema(models: Seq[Model]): Unit

  def createSchema(models: Seq[Model], vertexSrvs: Seq[VertexSrv[_]], edgeSrvs: Seq[EdgeSrv[_, _, _]])(implicit authContext: AuthContext): Unit = {
    createSchema(vertexSrvs.map(_.model) ++ edgeSrvs.map(_.model) ++ models)
    transaction { implicit graph ⇒
      vertexSrvs.foreach(_.createInitialValues())
    }
  }

  def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(this, graph)
    setSingleProperty(createdVertex, "_id", UUID.randomUUID, idMapping)
    setSingleProperty(createdVertex, "_createdAt", new Date, createdAtMapping)
    setSingleProperty(createdVertex, "_createdBy", authContext.userId, createdByMapping)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.toDomain(createdVertex)(this)
  }

  def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity): E with Entity = {
    val edgeMaybe = for {
      f ← graph.V().has(Key("_id") of from._id).headOption()
      t ← graph.V().has(Key("_id") of to._id).headOption()
    } yield {
      val createdEdge = model.create(e, f, t)(this, graph)
      setSingleProperty(createdEdge, "_id", UUID.randomUUID, idMapping)
      setSingleProperty(createdEdge, "_createdAt", new Date, createdAtMapping)
      setSingleProperty(createdEdge, "_createdBy", authContext.userId, createdByMapping)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.toDomain(createdEdge)(this)
    }
    edgeMaybe.getOrElse(sys.error("vertex not found"))
  }

  def update(graph: Graph, authContext: AuthContext, model: Model, id: String, fields: Map[FPath, UpdateOps.Type]): Unit = {
    val element: Element = model.get(id)(this, graph)
    setOptionProperty(element, "_updatedAt", Some(new Date), updatedAtMapping)
    setOptionProperty(element, "_updatedBy", Some(authContext.userId), updatedByMapping)
    fields.foreach {
      case (key, UpdateOps.SetAttribute(value)) ⇒
        val mapping = model.fields(key.toString).asInstanceOf[Mapping[Any, _, _]]
        setProperty(element, key.toString, value, mapping)
      case (_, UpdateOps.UnsetAttribute) ⇒ throw InternalError("Unset an attribute is not yet implemented")
      // TODO
    }
  }

  protected def getSingleProperty[D, G](element: Element, key: String, mapping: SingleMapping[D, G]): D = {
    val values = element.properties[G](key)
    if (values.hasNext) {
      val v = mapping.toDomain(values.next().value)
      if (values.hasNext)
        throw InternalError(s"Property $key must have only one value but is multivalued on element $element" + Model.printElement(element))
      else v
    } else throw InternalError(s"Property $key is missing on element $element" + Model.printElement(element))
  }

  protected def getOptionProperty[D, G](element: Element, key: String, mapping: OptionMapping[D, G]): Option[D] = {
    val values = element
      .properties[G](key)
    if (values.hasNext) {
      val v = Some(mapping.toDomain(values.next().value))
      if (values.hasNext)
        throw InternalError(s"Property $key must have at most one value but is multivalued on element $element" + Model.printElement(element))
      else v
    } else None
  }

  protected def getListProperty[D, G](element: Element, key: String, mapping: ListMapping[D, G]): Seq[D] =
    element
      .properties[G](key)
      .asScala
      .map(p ⇒ mapping.toDomain(p.value()))
      .toSeq

  protected def getSetProperty[D, G](element: Element, key: String, mapping: SetMapping[D, G]): Set[D] =
    element
      .properties[G](key)
      .asScala
      .map(p ⇒ mapping.toDomain(p.value()))
      .toSet

  def getProperty[D](element: Element, key: String, mapping: Mapping[D, _, _]): D =
    mapping match {
      case m: SingleMapping[_, _] ⇒ getSingleProperty(element, key, m).asInstanceOf[D]
      case m: OptionMapping[_, _] ⇒ getOptionProperty(element, key, m).asInstanceOf[D]
      case m: ListMapping[_, _]   ⇒ getListProperty(element, key, m).asInstanceOf[D]
      case m: SetMapping[_, _]    ⇒ getSetProperty(element, key, m).asInstanceOf[D]
    }

  protected def setSingleProperty[D, G](element: Element, key: String, value: D, mapping: SingleMapping[D, _]): Unit =
    mapping.toGraphOpt(value).foreach(element.property(key, _))

  protected def setOptionProperty[D, G](element: Element, key: String, value: Option[D], mapping: OptionMapping[D, _]): Unit =
    value.flatMap(mapping.toGraphOpt).foreach(element.property(key, _))

  protected def setListProperty[D, G](element: Element, key: String, values: Seq[D], mapping: ListMapping[D, _]): Unit = {
    element.properties(key).asScala.foreach(_.remove)
    values.flatMap(mapping.toGraphOpt).foreach(element.property(key, _))
  }

  protected def setSetProperty[D, G](element: Element, key: String, values: Set[D], mapping: SetMapping[D, _]): Unit = {
    element.properties(key).asScala.foreach(_.remove)
    values.flatMap(mapping.toGraphOpt).foreach(element.property(key, _))
  }

  def setProperty[D](element: Element, key: String, value: D, mapping: Mapping[D, _, _]): Unit =
    mapping match {
      case m: SingleMapping[d, _] ⇒ setSingleProperty(element, key, value, m)
      case m: OptionMapping[d, _] ⇒ setOptionProperty(element, key, value.asInstanceOf[Option[d]], m)
      case m: ListMapping[d, _]   ⇒ setListProperty(element, key, value.asInstanceOf[Seq[d]], m)
      case m: SetMapping[d, _]    ⇒ setSetProperty(element, key, value.asInstanceOf[Set[d]], m)

    }

  val chunkSize: Int = 32 * 1024

  def loadBinary(initialVertex: Vertex)(implicit graph: Graph): InputStream = loadBinary(initialVertex.value[String]("_id"))

  def loadBinary(id: String)(implicit graph: Graph): InputStream =
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

  def saveBinary(is: InputStream)(implicit graph: Graph): Vertex = {

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

    val chunks = Iterator
      .continually(readNextChunk)
      .takeWhile { x ⇒
        x.nonEmpty
      }
      .map { data ⇒
        val v = graph.addVertex("binary")
        v.property("binary", data)
        v
      }
    if (chunks.isEmpty) {
      logger.debug("Saving empty file")
      val v = graph.addVertex("binary")
      v.property("binary", "")
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
