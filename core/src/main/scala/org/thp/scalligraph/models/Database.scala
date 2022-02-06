package org.thp.scalligraph.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.{Order, P}
import org.apache.tinkerpop.gremlin.structure.Transaction.Status
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, T, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.traversal.Traversal.Identity
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Graph, Traversal}
import org.thp.scalligraph.{EntityId, InternalError, RichOptionTry, RichTryOption}
import play.api.Logger

import java.util.Date
import java.util.function.Consumer
import scala.collection.{AbstractIterator, Iterator, TraversableOnce}
import scala.util.{Success, Try}

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
  def addCallback(callback: () => Try[Unit])(implicit graph: Graph): Unit

  /** Must not be used outside the database */
  def takeCallbacks(graph: Graph): List[() => Try[Unit]]
  def addTransactionListener(listener: Consumer[Status])(implicit graph: Graph): Unit

  def indexCountQuery(graph: Graph, query: String): Long

  def version(module: String): Int
  def setVersion(module: String, v: Int): Try[Unit]
  val idMapping: Mapping[EntityId, EntityId, AnyRef]

  def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit]
  def createSchema(model: Model, models: Model*): Try[Unit] = createSchema(model +: models)
  def createSchema(models: Seq[Model]): Try[Unit]
  def addVertexModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit]
  def addEdgeModel(label: String, properties: Map[String, Mapping[_, _, _]]): Try[Unit]
  def addSchemaIndexes(schemaObject: Schema): Try[Boolean]
  def addSchemaIndexes(model: Model, models: Model*): Try[Boolean] = addSchemaIndexes(model +: models)
  def addSchemaIndexes(models: Seq[Model]): Try[Boolean]
  def addProperty(model: String, propertyName: String, mapping: Mapping[_, _, _]): Try[Unit]
  def removeProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean, mapping: Mapping[_, _, _]): Try[Unit]
  def addIndex(model: String, indexDefinition: Seq[(IndexType.Value, Seq[String])]): Try[Unit]
  def removeIndex(model: String, indexType: IndexType.Value, fields: Seq[String]): Try[Unit]
  def removeAllIndex(): Try[Unit]
  def reindexData(model: String): Try[Unit]
  def reindexData(): Unit
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

  def toId(id: Any): Any

  def labelFilter[D, G, C <: Converter[D, G]](label: String, traversal: Traversal[D, G, C]): Traversal[D, G, C]
  def mapPredicate[A](predicate: P[A]): P[A]
  def V[D <: Product](ids: EntityId*)(implicit model: Model.Vertex[D], graph: Graph): Traversal.V[D]
  def V(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Vertex, Vertex, Converter.Identity[Vertex]]
  def E[D <: Product](ids: EntityId*)(implicit model: Model.Edge[D], graph: Graph): Traversal.E[D]
  def E(label: String, ids: EntityId*)(implicit graph: Graph): Traversal[Edge, Edge, Converter.Identity[Edge]]
  def traversal()(implicit graph: Graph): GraphTraversalSource

  def pagedTraversal[R](
      pageSize: Int,
      filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex]
  )(
      process: Identity[Vertex] => Option[Try[R]]
  ): Iterator[Try[R]]

  def pagedTraversalIds[R](
      pageSize: Int,
      filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex]
  )(
      process: Seq[EntityId] => Option[R]
  ): Iterator[R]

  val extraModels: Seq[Model]
  val binaryLinkModel: Model.Edge[BinaryLink]
  val binaryModel: Model.Vertex[Binary]
}

abstract class BaseDatabase extends Database {
  override val createdAtMapping: SingleMapping[Date, Date]     = UMapping.date
  override val createdByMapping: SingleMapping[String, String] = UMapping.string
  override val updatedAtMapping: OptionMapping[Date, Date]     = UMapping.date.optional
  override val updatedByMapping: OptionMapping[String, String] = UMapping.string.optional

  override val binaryMapping: SingleMapping[Array[Byte], String] = UMapping.binary

  override def version(module: String): Int =
    roTransaction { graph =>
      logger.debug(s"Get version of $module")
      graph.variables.get[Int](s"${module}_version").orElse(0)
    }

  override def setVersion(module: String, v: Int): Try[Unit] =
    tryTransaction { graph =>
      logger.debug(s"Set version of $module to $v")
      Try(graph.variables.set(s"${module}_version", v))
    }

  override def transaction[A](body: Graph => A): A = tryTransaction(graph => Try(body(graph))).get

  private var callbacks: List[(AnyRef, () => Try[Unit])] = Nil

  def addCallback(callback: () => Try[Unit])(implicit graph: Graph): Unit =
    synchronized {
      callbacks = (graph -> callback) :: callbacks
    }

  def takeCallbacks(graph: Graph): List[() => Try[Unit]] =
    synchronized {
      val (cb, updatedCallbacks) = callbacks.partition(_._1 == graph)
      callbacks = updatedCallbacks
      cb
    }.map(_._2)

  override def addTransactionListener(listener: Consumer[Status])(implicit graph: Graph): Unit = graph.addTransactionListener(listener)

  override def createSchemaFrom(schemaObject: Schema)(implicit authContext: AuthContext): Try[Unit] =
    createSchema(schemaObject.modelList ++ extraModels)
      .map { _ =>
        schemaObject.modelList.foreach {
          case model: Model.Vertex[_] =>
            tryTransaction { graph =>
              model.initialValues.foreach(createVertex(graph, authContext, model, _))
              Success(())
            }
          case _ => Nil
        }
      }

  override def addSchemaIndexes(schemaObject: Schema): Try[Boolean] = addSchemaIndexes(schemaObject.modelList ++ extraModels)

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
      f <- graph.V(from._label, from._id).headOption
      t <- graph.V(to._label, to._id).headOption
    } yield {
      val createdEdge = model.create(e, f, t)(graph)
      val entity      = DummyEntity(model.label, createdEdge.id(), authContext.userId)
      createdAtMapping.setProperty(createdEdge, "_createdAt", entity._createdAt)
      createdByMapping.setProperty(createdEdge, "_createdBy", entity._createdBy)
      logger.trace(s"Create edge ${model.label} from $f to $t: ${Model.printElement(createdEdge)}")
      model.addEntity(e, entity)
    }
    edgeMaybe.getOrElse {
      val error = graph.V(from._label, from._id).headOption.map(_ => "").getOrElse(s"${from._label}:${from._id} not found ") +
        graph.V(to._label, to._id).headOption.map(_ => "").getOrElse(s"${to._label}:${to._id} not found")
      throw InternalError(s"Fail to create edge between ${from._label}:${from._id} and ${to._label}:${to._id}, $error")
    }
  }

  val chunkSize: Int = 32 * 1024

  override val binaryLinkModel: Model.Edge[BinaryLink] = BinaryLink.model
  override val binaryModel: Model.Vertex[Binary]       = Binary.model
  def labelFilter[D, G <: Element, C <: Converter[D, G]](label: String): Traversal[D, G, C] => Traversal[D, G, C] =
    _.onRaw(_.hasLabel(label))
  override def mapPredicate[A](predicate: P[A]): P[A] = predicate
  override def toId(id: Any): Any                     = id

  override val extraModels: Seq[Model] = Seq(binaryModel, binaryLinkModel)

  override def pagedTraversal[R](
      pageSize: Int,
      filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex]
  )(
      process: Identity[Vertex] => Option[Try[R]]
  ): Iterator[Try[R]] =
    pagedTraversalIds(pageSize, filter) { ids =>
      tryTransaction { implicit graph =>
        process(graph.VV(ids: _*)).flip
      }.flip
    }

  final private class UnfoldIterator[A, S](init: Option[S])(f: Option[S] => Option[(A, S)]) extends AbstractIterator[A] {
    private[this] var state: Option[S]           = init
    private[this] var nextResult: Option[(A, S)] = null

    override def hasNext: Boolean = {
      if (nextResult eq null) {
        nextResult = {
          val res = f(state)
          if (res eq null) throw new NullPointerException("null during unfold")
          res
        }
        state = null.asInstanceOf[Option[S]] // allow GC
      }
      nextResult.isDefined
    }

    override def next(): A =
      if (hasNext) {
        val (value, newState) = nextResult.get
        state = Some(newState)
        nextResult = null
        value
      } else Iterator.empty.next()
  }

  override def pagedTraversalIds[R](
      pageSize: Int,
      filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex]
  )(
      process: Seq[EntityId] => Option[R]
  ): Iterator[R] = {
    require(pageSize > 1)
    def unfold[A, B](init: Option[A])(f: Option[A] => Option[(B, A)]) = new UnfoldIterator(init)(f)
    def getFirstPage(count: Int)(implicit graph: Graph): Seq[EntityId] =
      filter(graph.VV())
        .sort(_.by("_createdAt", Order.desc))
        .limit(count.toLong)
        ._id
        .toSeq
    def getPage(date: Date, excludeIds: Seq[EntityId], count: Int)(implicit graph: Graph): Seq[EntityId] =
      filter(graph.VV())
        .unsafeHas("_createdAt", P.lte(date))
        .has(T.id, P.without(excludeIds.map(idMapping.reverse): _*))
        .sort(_.by("_createdAt", Order.desc))
        .limit(count.toLong)
        ._id
        .toSeq
    def getPageOfDate(date: Date, excludeIds: Seq[EntityId])(implicit graph: Graph): Seq[EntityId] =
      filter(graph.VV())
        .unsafeHas("_createdAt", P.eq(date))
        .has(T.id, P.without(excludeIds.map(idMapping.reverse): _*))
        ._id
        .toSeq
    def getDate(id: EntityId)(implicit graph: Graph): Date = graph.VV(id).property("_createdAt", UMapping.date).head

    // Iterator.unfold[(Date, Seq[String]), TraversableOnce[R]](None) { refDate =>
    unfold[(Date, Seq[EntityId]), TraversableOnce[R]](None) { state =>
      roTransaction { implicit graph =>
        val ids = state.fold(getFirstPage(pageSize))(s => getPage(s._1, s._2, pageSize))

        if (ids.isEmpty) None
        else {
          val lastId   = ids.last
          val lastDate = getDate(lastId)
          if (ids.size > 1 && lastDate == getDate(ids.head))
            Some((getPageOfDate(lastDate, state.fold(Seq.empty[EntityId])(_._2)), new Date(lastDate.getTime - 1), Nil))
          else {
            val reverseIds = ids.reverseIterator
            reverseIds.next()
            val sameDateIds = reverseIds.takeWhile(getDate(_) == lastDate).toVector :+ lastId
            Some((ids, lastDate, sameDateIds))
          }
        }
      }.flatMap {
        case (ids, lastDate, sameDateIds) =>
          if (ids.size <= pageSize)
            process(ids).map { r =>
              (Seq(r), (lastDate, sameDateIds))
            }
          else
            ids
              .grouped(pageSize)
              .foldLeft[Option[List[R]]](Some(Nil))((a, i) => a.flatMap(r => process(i).map(_ :: r)))
              .map(r => (r, (new Date(lastDate.getTime - 1), sameDateIds)))
      }
    }.flatten
  }
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
