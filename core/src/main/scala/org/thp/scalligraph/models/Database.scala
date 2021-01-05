package org.thp.scalligraph.models

import java.util.Date
import java.util.function.Consumer
import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.Transaction.Status
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Vertex}
import org.thp.scalligraph.{EntityId, InternalError}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Graph, Traversal}
import play.api.Logger

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.util.Try

class DatabaseException(message: String = "Violation of database schema", cause: Exception) extends Exception(message, cause)

trait Database {
  lazy val logger: Logger = Logger("org.thp.scalligraph.models.Database")
  val createdAtMapping: SingleMapping[Date, _]
  val createdByMapping: SingleMapping[String, String]
  val updatedAtMapping: OptionMapping[Date, _]
  val updatedByMapping: OptionMapping[String, String]
  val binaryMapping: SingleMapping[Array[Byte], String]

  def close(): Unit
  def roTransaction[A](body: Graph => A): A
  def source[A](query: Graph => Iterator[A]): Source[A, NotUsed]
  def source[A, B](body: Graph => (Iterator[A], B)): (Source[A, NotUsed], B)

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
  val idMapping: Mapping[EntityId, EntityId, AnyRef]

  def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit]
  def createSchema(model: Model, models: Model*): Try[Unit] = createSchema(model +: models)
  def createSchema(models: Seq[Model]): Try[Unit]
  def addVertexModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit]
  def addEdgeModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit]
  def addSchemaIndexes(schemaObject: Schema): Try[Unit]
  def addSchemaIndexes(model: Model, models: Model*): Try[Unit] = addSchemaIndexes(model +: models)
  def addSchemaIndexes(models: Seq[Model]): Try[Unit]
  def addProperty(model: String, propertyName: String, mapping: Mapping[_, _, _]): Try[Unit]
  def removeProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean): Try[Unit]
  def addIndex(model: String, indexDefinition: Seq[(IndexType.Value, Seq[String])]): Try[Unit]
  def enableIndexes(): Try[Unit]
  def removeAllIndexes(): Unit
//  def removeIndex(model: String, properties: Seq[String]): Try[Unit]
  val fullTextIndexAvailable: Boolean

  def drop(): Unit

  def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity

  def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity

//  def getAllProperties(element: Element): Map[String, Seq[Any]] = {
//    val m = mutable.Map.empty[String, mutable.Builder[Any, Seq[Any]]]
//    for (property <- element.properties[Any]().asScala) {
//      val bldr = m.getOrElseUpdate(property.key(), Seq.newBuilder)
//      bldr += property.value()
//    }
//    val b = immutable.Map.newBuilder[String, Seq[Any]]
//    for ((k, v) <- m)
//      b += ((k, v.result))
//    b.result
//  }
//  def getSingleProperty[D, G](element: Element, key: String, mapping: SingleMapping[D, G]): D
//  def getOptionProperty[D, G](element: Element, key: String, mapping: OptionMapping[D, G]): Option[D]
//  def getListProperty[D, G](element: Element, key: String, mapping: ListMapping[D, G]): Seq[D]
//  def getSetProperty[D, G](element: Element, key: String, mapping: SetMapping[D, G]): Set[D]
//
//  def getProperty[D](element: Element, key: String, mapping: Mapping[D, _, _]): D =
//    mapping match {
//      case m: SingleMapping[_, _] => getSingleProperty(element, key, m).asInstanceOf[D]
//      case m: OptionMapping[_, _] => getOptionProperty(element, key, m).asInstanceOf[D]
//      case m: ListMapping[_, _]   => getListProperty(element, key, m).asInstanceOf[D]
//      case m: SetMapping[_, _]    => getSetProperty(element, key, m).asInstanceOf[D]
//    }
//
//  def setSingleProperty[D, G](element: Element, key: String, value: D, mapping: SingleMapping[D, _]): Unit
//  def setOptionProperty[D, G](element: Element, key: String, value: Option[D], mapping: OptionMapping[D, _]): Unit
//  def setListProperty[D, G](element: Element, key: String, values: Seq[D], mapping: ListMapping[D, _]): Unit
//  def setSetProperty[D, G](element: Element, key: String, values: Set[D], mapping: SetMapping[D, _]): Unit
//
//  def setProperty[D](element: Element, key: String, value: D, mapping: Mapping[D, _, _]): Unit = {
//    logger.trace(s"set ${element.id()}, $key = $value")
//    mapping match {
//      case m: SingleMapping[d, _] => setSingleProperty(element, key, value, m)
//      case m: OptionMapping[d, _] => setOptionProperty(element, key, value.asInstanceOf[Option[d]], m)
//      case m: ListMapping[d, _]   => setListProperty(element, key, value.asInstanceOf[Seq[d]], m)
//      case m: SetMapping[d, _]    => setSetProperty(element, key, value.asInstanceOf[Set[d]], m)
//    }
//  }

//  def mapPredicate[T](predicate: P[T]): P[T]
  def toId(id: Any): Any

//  @deprecated("Use V(model) or E(model)", "0.2")
//  def labelFilter[D, G <: Element, C <: Converter[D, G]](model: Model): Traversal[D, G, C] => Traversal[D, G, C] = labelFilter(model.label)
  def labelFilter[D, G, C <: Converter[D, G]](label: String, traversal: Traversal[D, G, C]): Traversal[D, G, C]
  def V[D <: Product](ids: EntityId*)(implicit model: Model.Vertex[D], graph: Graph): Traversal.V[D]
  def V(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Vertex, Vertex, Converter.Identity[Vertex]]
  def E[D <: Product](ids: EntityId*)(implicit model: Model.Edge[D], graph: Graph): Traversal.E[D]
  def E(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Edge, Edge, Converter.Identity[Edge]]

  val extraModels: Seq[Model]
  val binaryLinkModel: Model.Edge[BinaryLink]
  val binaryModel: Model.Vertex[Binary]

  var printByteCode: Boolean   = false
  var printStrategies: Boolean = true
  var printExplain: Boolean    = false
  var printProfile: Boolean    = true
}

abstract class BaseDatabase extends Database {
  override val createdAtMapping: SingleMapping[Date, Date]     = UMapping.date
  override val createdByMapping: SingleMapping[String, String] = UMapping.string
  override val updatedAtMapping: OptionMapping[Date, Date]     = UMapping.date.optional
  override val updatedByMapping: OptionMapping[String, String] = UMapping.string.optional

  override val binaryMapping: SingleMapping[Array[Byte], String] = UMapping.binary

  override def version(module: String): Int = roTransaction(graph => graph.variables.get[Int](s"${module}_version").orElse(0))

  override def setVersion(module: String, v: Int): Try[Unit] = tryTransaction(graph => Try(graph.variables.set(s"${module}_version", v)))

  override def transaction[A](body: Graph => A): A = tryTransaction(graph => Try(body(graph))).get

  private var callbacks: List[(AnyRef, () => Try[Unit])] = Nil

  def addCallback(callback: () => Try[Unit])(implicit graph: Graph): Unit =
    synchronized {
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

  override def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit] =
    for {
      _ <- createSchema(schemaObject.modelList ++ extraModels)
      _ <- tryTransaction(graph => Try(schemaObject.initialValues.foreach(_.create()(graph, authContext))))
      _ <- tryTransaction(graph => schemaObject.init(this)(graph, authContext))
    } yield ()

  override def addSchemaIndexes(schemaObject: Schema): Try[Unit] = addSchemaIndexes(schemaObject.modelList ++ extraModels)

  protected case class DummyEntity(
      _label: String,
      id: AnyRef,
      _createdBy: String,
      _createdAt: Date = new Date,
      _updatedBy: Option[String] = None,
      _updatedAt: Option[Date] = None
  ) extends Entity {
    val _id: EntityId = EntityId(id)
  }

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity = {
    val createdVertex = model.create(v)(graph)
    val entity        = DummyEntity(model.label, createdVertex.id(), authContext.userId)
    createdAtMapping.setProperty(createdVertex, "_createdAt", entity._createdAt)
    createdByMapping.setProperty(createdVertex, "_createdBy", entity._createdBy)
    logger.trace(s"Created vertex is ${Model.printElement(createdVertex)}")
    model.addEntity(v, entity)
  }

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E],
      e: E,
      from: FROM with Entity,
      to: TO with Entity
  ): E with Entity = {
    val edgeMaybe = for {
      f <- Traversal.V(from._id)(graph).headOption
      t <- Traversal.V(to._id)(graph).headOption
    } yield {
      val createdEdge = model.create(e, f, t)(graph)
      val entity      = DummyEntity(model.label, createdEdge.id(), authContext.userId)
      createdAtMapping.setProperty(createdEdge, "_createdAt", entity._createdAt)
      createdByMapping.setProperty(createdEdge, "_createdBy", entity._createdBy)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.addEntity(e, entity)
    }
    edgeMaybe.getOrElse {
      val error = Traversal.V(from._id)(graph).headOption.map(_ => "").getOrElse(s"${from._label}:${from._id} not found ") +
        Traversal.V(to._id)(graph).headOption.map(_ => "").getOrElse(s"${to._label}:${to._id} not found")
      throw InternalError(s"Fail to create edge between ${from._label}:${from._id} and ${to._label}:${to._id}, $error")
    }
  }

  val chunkSize: Int = 32 * 1024

  override val binaryLinkModel: Model.Edge[BinaryLink] = BinaryLink.model
  override val binaryModel: Model.Vertex[Binary]       = Binary.model
  def labelFilter[D, G <: Element, C <: Converter[D, G]](label: String): Traversal[D, G, C] => Traversal[D, G, C] =
    _.onRaw(_.hasLabel(label))
  override def toId(id: Any): Any = id

  override val extraModels: Seq[Model] = Seq(binaryModel, binaryLinkModel)

}
case class Binary(attachmentId: Option[String], folder: Option[String], data: Array[Byte])
object Binary {
  val model: Model.Vertex[Binary] = new VertexModel {
    override type E = Binary
    override val label: String                                = "Binary"
    override val indexes: Seq[(IndexType.Value, Seq[String])] = Nil
    override val fields: Map[String, Mapping[_, _, _]] =
      Map("data" -> UMapping.binary, "folder" -> UMapping.string.optional, "attachmentId" -> UMapping.string.optional)

    override def create(binary: Binary)(implicit graph: Graph): Vertex = {
      val v = graph.addVertex(label)
      UMapping.binary.setProperty(v, "data", binary.data)
      UMapping.string.optional.setProperty(v, "folder", binary.folder)
      UMapping.string.optional.setProperty(v, "attachmentId", binary.attachmentId)
      v
    }

    override val converter: Converter[Binary with Entity, Vertex] = (element: Vertex) => {
      val data         = UMapping.binary.getProperty(element, "data")
      val folder       = UMapping.string.optional.getProperty(element, "folder")
      val attachmentId = UMapping.string.optional.getProperty(element, "attachmentId")

      new Binary(attachmentId, folder, data) with Entity {
        override def _id: EntityId              = EntityId(element.id())
        override val _label: String             = "Binary"
        override def _createdBy: String         = ""
        override def _updatedBy: Option[String] = None
        override def _createdAt: Date           = new Date
        override def _updatedAt: Option[Date]   = None
      }
    }

    override def addEntity(binary: Binary, entity: Entity): EEntity =
      new Binary(binary.attachmentId, binary.folder, binary.data) with Entity {
        override def _id: EntityId              = entity._id
        override def _label: String             = entity._label
        override def _createdBy: String         = entity._createdBy
        override def _updatedBy: Option[String] = entity._updatedBy
        override def _createdAt: Date           = entity._createdAt
        override def _updatedAt: Option[Date]   = entity._updatedAt
      }
  }
}
case class BinaryLink()
object BinaryLink {
  lazy val model: Model.Edge[BinaryLink] = new EdgeModel {
    override def create(e: BinaryLink, from: Vertex, to: Vertex)(implicit graph: Graph): Edge = from.addEdge(label, to)
    override type E = BinaryLink
    override val label: String                                = "NextChunk"
    override val indexes: Seq[(IndexType.Value, Seq[String])] = Nil
    override val fields: Map[String, Mapping[_, _, _]]        = Map.empty

    override val converter: Converter[BinaryLink with Entity, Edge] = (element: Edge) =>
      new BinaryLink with Entity {
        override def _id: EntityId              = EntityId(element.id())
        override val _label: String             = "NextChunk"
        override def _createdBy: String         = ""
        override def _updatedBy: Option[String] = None
        override def _createdAt: Date           = new Date
        override def _updatedAt: Option[Date]   = None
      }

    override def addEntity(binaryLink: BinaryLink, entity: Entity): EEntity =
      new BinaryLink with Entity {
        override def _id: EntityId              = entity._id
        override def _label: String             = entity._label
        override def _createdBy: String         = entity._createdBy
        override def _updatedBy: Option[String] = entity._updatedBy
        override def _createdAt: Date           = entity._createdAt
        override def _updatedAt: Option[Date]   = entity._updatedAt
      }
  }
}
