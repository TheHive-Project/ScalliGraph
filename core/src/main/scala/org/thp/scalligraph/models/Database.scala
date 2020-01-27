package org.thp.scalligraph.models

import java.util.function.Consumer
import java.util.{Base64, Date}

import gremlin.scala._
import org.apache.tinkerpop.gremlin.structure.Transaction.Status
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.FAny
import org.thp.scalligraph.{InternalError, RichSeq, UnknownAttributeError}
import play.api.Logger

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

class DatabaseException(message: String = "Violation of database schema", cause: Exception) extends Exception(message, cause)

trait Database {
  lazy val logger: Logger = Logger("org.thp.scalligraph.models.Database")
  val createdAtMapping: SingleMapping[Date, _]
  val createdByMapping: SingleMapping[String, String]
  val updatedAtMapping: OptionMapping[Date, _]
  val updatedByMapping: OptionMapping[String, String]
  val binaryMapping: SingleMapping[Array[Byte], String]

  def roTransaction[A](body: Graph => A): A

  @deprecated("Use tryTransaction", "0.2")
  def transaction[A](body: Graph => A): A
  def tryTransaction[A](body: Graph => Try[A]): Try[A]
  def currentTransactionId(graph: Graph): AnyRef
  def addCallback(callback: () => Try[Unit])(implicit graph: Graph): Unit

  /** Must not be used outside the database */
  def takeCallbacks(graph: Graph): List[() => Try[Unit]]
  def addTransactionListener(listener: Consumer[Status])(implicit graph: Graph): Unit

  def version(module: String): Int
  def setVersion(module: String, v: Int): Try[Unit]
  def isValidId(id: String): Boolean

  def getModel[E <: Product: ru.TypeTag]: Model.Base[E]
  def getVertexModel[E <: Product: ru.TypeTag]: Model.Vertex[E]
  def getEdgeModel[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product]: Model.Edge[E, FROM, TO]

  def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit]
  def createSchema(model: Model, models: Model*): Try[Unit] = createSchema(model +: models)
  def createSchema(models: Seq[Model]): Try[Unit]

  def drop(): Unit

  def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity

  def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity

  def update[E <: Product](
      elementTraversal: GremlinScala[_ <: Element],
      fields: Seq[(String, Any)],
      model: Model.Base[E],
      graph: Graph,
      authContext: AuthContext
  ): Try[Seq[E with Entity]]

  def getSingleProperty[D, G](element: Element, key: String, mapping: SingleMapping[D, G]): D
  def getOptionProperty[D, G](element: Element, key: String, mapping: OptionMapping[D, G]): Option[D]
  def getListProperty[D, G](element: Element, key: String, mapping: ListMapping[D, G]): Seq[D]
  def getSetProperty[D, G](element: Element, key: String, mapping: SetMapping[D, G]): Set[D]

  def getProperty[D](element: Element, key: String, mapping: Mapping[D, _, _]): D =
    mapping match {
      case m: SingleMapping[_, _] => getSingleProperty(element, key, m).asInstanceOf[D]
      case m: OptionMapping[_, _] => getOptionProperty(element, key, m).asInstanceOf[D]
      case m: ListMapping[_, _]   => getListProperty(element, key, m).asInstanceOf[D]
      case m: SetMapping[_, _]    => getSetProperty(element, key, m).asInstanceOf[D]
    }

  def setSingleProperty[D, G](element: Element, key: String, value: D, mapping: SingleMapping[D, _]): Unit
  def setOptionProperty[D, G](element: Element, key: String, value: Option[D], mapping: OptionMapping[D, _]): Unit
  def setListProperty[D, G](element: Element, key: String, values: Seq[D], mapping: ListMapping[D, _]): Unit
  def setSetProperty[D, G](element: Element, key: String, values: Set[D], mapping: SetMapping[D, _]): Unit

  def setProperty[D](element: Element, key: String, value: D, mapping: Mapping[D, _, _]): Unit = {
    logger.trace(s"set ${element.id()}, $key = $value")
    mapping match {
      case m: SingleMapping[d, _] => setSingleProperty(element, key, value, m)
      case m: OptionMapping[d, _] => setOptionProperty(element, key, value.asInstanceOf[Option[d]], m)
      case m: ListMapping[d, _]   => setListProperty(element, key, value.asInstanceOf[Seq[d]], m)
      case m: SetMapping[d, _]    => setSetProperty(element, key, value.asInstanceOf[Set[d]], m)
    }
  }

  def mapPredicate[T](predicate: P[T]): P[T]
  def toId(id: Any): Any
  def labelFilter[E <: Element](model: Model): GremlinScala[E] => GremlinScala[E] = labelFilter(model.label)
  def labelFilter[E <: Element](label: String): GremlinScala[E] => GremlinScala[E]

  val extraModels: Seq[Model]
}

abstract class BaseDatabase extends Database {
  val createdAtMapping: SingleMapping[Date, Date]     = UniMapping.date
  val createdByMapping: SingleMapping[String, String] = UniMapping.string
  val updatedAtMapping: OptionMapping[Date, Date]     = UniMapping.date.optional
  val updatedByMapping: OptionMapping[String, String] = UniMapping.string.optional

  val binaryMapping: SingleMapping[Array[Byte], String] =
    SingleMapping[Array[Byte], String](data => Some(Base64.getEncoder.encodeToString(data)), Base64.getDecoder.decode)

  override def version(module: String): Int = roTransaction(graph => graph.variables.get[Int](s"${module}_version").orElse(0))

  override def setVersion(module: String, v: Int): Try[Unit] = tryTransaction(graph => Try(graph.variables.set(s"${module}_version", v)))

  override def transaction[A](body: Graph => A): A = tryTransaction(graph => Try(body(graph))).get

  private var callbacks: List[(AnyRef, () => Try[Unit])] = Nil

  def addCallback(callback: () => Try[Unit])(implicit graph: Graph): Unit = synchronized {
    callbacks = (currentTransactionId(graph) -> callback) :: callbacks
  }

  def takeCallbacks(graph: Graph): List[() => Try[Unit]] = {
    val tx = currentTransactionId(graph)
    synchronized {
      val (cb, updatedCallbacks) = callbacks.partition(_._1 == tx)
      callbacks = updatedCallbacks
      cb
    }.map(_._2)
  }

  override def addTransactionListener(listener: Consumer[Status])(implicit graph: Graph): Unit = graph.tx().addTransactionListener(listener)

  override def getModel[E <: Product: ru.TypeTag]: Model.Base[E] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val companionMirror =
      rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule)
    companionMirror.instance match {
      case hm: HasModel[_] => hm.model.asInstanceOf[Model.Base[E]]
      case _               => throw InternalError(s"Class ${companionMirror.symbol} is not a model")
    }
  }

  override def getVertexModel[E <: Product: ru.TypeTag]: Model.Vertex[E] = {
    val rm              = ru.runtimeMirror(getClass.getClassLoader)
    val classMirror     = rm.reflectClass(ru.typeOf[E].typeSymbol.asClass)
    val companionSymbol = classMirror.symbol.companion
    val companionMirror = rm.reflectModule(companionSymbol.asModule)
    companionMirror.instance match {
      case hm: HasVertexModel[_] => hm.model.asInstanceOf[Model.Vertex[E]]
      case _                     => throw InternalError(s"Class ${companionMirror.symbol} is not a vertex model")
    }
  }

  override def getEdgeModel[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product]: Model.Edge[E, FROM, TO] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val companionMirror =
      rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule)
    companionMirror.instance match {
      case hm: HasEdgeModel[_, _, _] => hm.model.asInstanceOf[Model.Edge[E, FROM, TO]]
      case _                         => throw InternalError(s"Class ${companionMirror.symbol} is not an edge model")
    }
  }

  override def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit] =
    for {
      _ <- createSchema(schemaObject.modelList ++ extraModels)
      _ <- tryTransaction(graph => Try(schemaObject.initialValues.foreach(_.create()(this, graph, authContext))))
      _ <- tryTransaction(graph => schemaObject.init(graph, authContext))
    } yield ()

  override def createSchema(models: Seq[Model]): Try[Unit]

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(this, graph)
    setSingleProperty(createdVertex, "_createdAt", new Date, createdAtMapping)
    setSingleProperty(createdVertex, "_createdBy", authContext.userId, createdByMapping)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.toDomain(createdVertex)(this)
  }

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity = {
    val edgeMaybe = for {
      f <- graph.V(from._id).headOption()
      t <- graph.V(to._id).headOption()
    } yield {
      val createdEdge = model.create(e, f, t)(this, graph)
      setSingleProperty(createdEdge, "_createdAt", new Date, createdAtMapping)
      setSingleProperty(createdEdge, "_createdBy", authContext.userId, createdByMapping)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.toDomain(createdEdge)(this)
    }
    edgeMaybe.getOrElse {
      val error = graph.V(from._id).headOption().map(_ => "").getOrElse(s"${from._model.label}:${from._id} not found ") +
        graph.V(to._id).headOption().map(_ => "").getOrElse(s"${to._model.label}:${to._id} not found")
      throw InternalError(s"Fail to create edge between ${from._model.label}:${from._id} and ${to._model.label}:${to._id}, $error")
    }
  }

  override def update[E <: Product](
      elementTraversal: GremlinScala[_ <: Element],
      fields: Seq[(String, Any)],
      model: Model.Base[E],
      graph: Graph,
      authContext: AuthContext
  ): Try[Seq[E with Entity]] = {

    def getFieldMapping(key: String, value: Any): Try[Mapping[_, _, _]] =
      model
        .fields
        .get(key)
        .fold[Try[Mapping[_, _, _]]](Failure(UnknownAttributeError(key, FAny(Seq(value.toString)))))(Success.apply)

    logger.debug(s"Execution of $elementTraversal (update)")
    elementTraversal
      .traversal
      .asScala
      .toTry { element =>
        logger.trace(s"Update ${element.id()} by ${authContext.userId}")
        fields
          .toTry {
            case (key, value) =>
              getFieldMapping(key, value)
                .map { mapping => // TODO check if value has the correct type
                  setProperty(element, key, value, mapping.asInstanceOf[Mapping[Any, _, _]])
                  logger.trace(s" - $key = $value")
                }
          }
          .map { _ =>
            setOptionProperty(element, "_updatedAt", Some(new Date), updatedAtMapping)
            setOptionProperty(element, "_updatedBy", Some(authContext.userId), updatedByMapping)
            model.toDomain(element.asInstanceOf[model.ElementType])(this)
          }
      }
  }

  override def getSingleProperty[D, G](element: Element, key: String, mapping: SingleMapping[D, G]): D = {
    val values = element.properties[G](key)
    if (values.hasNext) {
      val v = mapping.toDomain(values.next().value)
      if (values.hasNext)
        throw InternalError(s"Property $key must have only one value but is multivalued on element $element" + Model.printElement(element))
      else v
    } else throw InternalError(s"Property $key is missing on element $element" + Model.printElement(element))
  }

  override def getOptionProperty[D, G](element: Element, key: String, mapping: OptionMapping[D, G]): Option[D] = {
    val values = element
      .properties[G](key)
    if (values.hasNext) {
      val v = Some(mapping.toDomain(values.next().value))
      if (values.hasNext)
        throw InternalError(s"Property $key must have at most one value but is multivalued on element $element" + Model.printElement(element))
      else v
    } else None
  }

  override def getListProperty[D, G](element: Element, key: String, mapping: ListMapping[D, G]): Seq[D] =
    element
      .properties[G](key)
      .asScala
      .map(p => mapping.toDomain(p.value()))
      .toList

  override def getSetProperty[D, G](element: Element, key: String, mapping: SetMapping[D, G]): Set[D] =
    element
      .properties[G](key)
      .asScala
      .map(p => mapping.toDomain(p.value()))
      .toSet

  override def setSingleProperty[D, G](element: Element, key: String, value: D, mapping: SingleMapping[D, _]): Unit =
    mapping.toGraphOpt(value).foreach(element.property(key, _))

  override def setOptionProperty[D, G](element: Element, key: String, value: Option[D], mapping: OptionMapping[D, _]): Unit = {
    value.flatMap(mapping.toGraphOpt) match {
      case Some(v) => element.property(key, v)
      case None    => element.property(key).remove()
    }
    ()
  }

  override def setListProperty[D, G](element: Element, key: String, values: Seq[D], mapping: ListMapping[D, _]): Unit = {
    element.properties(key).asScala.foreach(_.remove)
    element match {
      case vertex: Vertex => values.flatMap(mapping.toGraphOpt).foreach(vertex.property(Cardinality.list, key, _))
      case _              => throw InternalError("Edge doesn't support multi-valued properties")
    }
  }

  override def setSetProperty[D, G](element: Element, key: String, values: Set[D], mapping: SetMapping[D, _]): Unit = {
    element.properties(key).asScala.foreach(_.remove)
    element match {
      case vertex: Vertex => values.flatMap(mapping.toGraphOpt).foreach(vertex.property(Cardinality.set, key, _))
      case _              => throw InternalError("Edge doesn't support multi-valued properties")
    }
  }

  val chunkSize: Int = 32 * 1024

  val binaryLinkModel: EdgeModel[Binary, Binary] = new EdgeModel[Binary, Binary] {
    override val fromLabel: String                                                                       = "binaryData"
    override val toLabel: String                                                                         = "binaryData"
    override def create(e: Product, from: Vertex, to: Vertex)(implicit db: Database, graph: Graph): Edge = from.addEdge(label, to)
    override type E = Product
    override val label: String                                = "nextChunk"
    override val indexes: Seq[(IndexType.Value, Seq[String])] = Nil
    override val fields: Map[String, Mapping[_, _, _]]        = Map.empty

    override def toDomain(element: Edge)(implicit db: Database): Product with Entity = new Product with Entity {
      override val _id: String                  = element.id().toString
      override val _model: Model                = binaryLinkModel
      override val _createdBy: String           = "system"
      override val _updatedBy: Option[String]   = None
      override val _createdAt: Date             = new Date
      override val _updatedAt: Option[Date]     = None
      override def productElement(n: Int): Any  = ()
      override def productArity: Int            = 0
      override def canEqual(that: Any): Boolean = false
    }
  }

  val binaryModel: Model.Vertex[Binary] = new VertexModel {

    override def create(binary: Binary)(implicit db: Database, graph: Graph): Vertex = {
      val v    = graph.addVertex("binary")
      val data = Base64.getEncoder.encodeToString(binary.data)
      setSingleProperty(v, "binary", data, UniMapping.string)
      v
    }
    override type E = Binary
    override val label: String                                = "binary"
    override val indexes: Seq[(IndexType.Value, Seq[String])] = Nil
    override val fields: Map[String, Mapping[_, _, _]]        = Map("binary" -> UniMapping.string)

    override def toDomain(element: Vertex)(implicit db: Database): Binary with Entity = {
      val base64Data = getSingleProperty[String, String](element, "binary", UniMapping.string)
      val data       = Base64.getDecoder.decode(base64Data)
      new Binary(None, None, data) with Entity {
        override val _id: String                = element.id().toString
        override val _model: Model              = binaryModel
        override val _createdBy: String         = "system"
        override val _updatedBy: Option[String] = None
        override val _createdAt: Date           = new Date
        override val _updatedAt: Option[Date]   = None
      }
    }
  }

  override def labelFilter[E <: Element](label: String): GremlinScala[E] => GremlinScala[E] = _.hasLabel(label)
  override def mapPredicate[T](predicate: P[T]): P[T]                                       = predicate
  override def toId(id: Any): Any                                                           = id

  override val extraModels: Seq[Model] = Seq(binaryModel, binaryLinkModel)

}
case class Binary(attachmentId: Option[String], folder: Option[String], data: Array[Byte])
