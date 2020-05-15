package org.thp.scalligraph.steps

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, UUID, Collection => JCollection, List => JList, Map => JMap}

import gremlin.scala.dsl.Converter
import gremlin.scala.{By, Edge, Graph, GremlinScala, Key, OrderBy, P, ProjectionBuilder, StepLabel, Vertex, __, _}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick
import org.apache.tinkerpop.gremlin.structure.Element
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.Renderer
import org.thp.scalligraph.models.{Entity, Mapping, UniMapping}
import org.thp.scalligraph.services.VertexSrv
import org.thp.scalligraph.{AuthorizationError, InternalError, NotFoundError}
import play.api.Logger
import shapeless.HNil

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

trait BranchOption[T <: BaseTraversal, R <: BaseTraversal] {
  def traversal: T => R
  def pickToken: Any
}

case class BranchCase[BranchOn, T <: BaseTraversal, R <: BaseTraversal](pickToken: BranchOn, traversal: T => R) extends BranchOption[T, R]

case class BranchMatchAll[T <: BaseTraversal, R <: BaseTraversal](traversal: T => R) extends BranchOption[T, R] {
  override def pickToken = Pick.any
}

/* if nothing else matched in branch/choose step */
case class BranchOtherwise[T <: BaseTraversal, R <: BaseTraversal](traversal: T => R) extends BranchOption[T, R] {
  override def pickToken = Pick.none
}

object StepsOps {

  private lazy val logger: Logger = Logger(classOf[Traversal[_, _]])

  def union[T <: VertexSteps[_]](
      srv: VertexSrv[_, T]
  )(firstTraversal: GremlinScala[Vertex] => T, otherTraverals: (GremlinScala[Vertex] => T)*)(implicit graph: Graph): T = {
    val traversals = (firstTraversal +: otherTraverals)
      .map(t => (g: GremlinScala.Aux[Int, HNil]) => t(GremlinScala[Vertex, HNil](g.traversal.V())).raw)
    srv.steps(graph.inject(1).unionFlat(traversals: _*))
  }

  def onlyOneOf[A](elements: JList[A]): A = {
    val size = elements.size
    if (size == 1) elements.get(0)
    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
    else throw InternalError("No element found")
  }

  def atMostOneOf[A](elements: JList[A]): Option[A] = {
    val size = elements.size
    if (size == 1) Some(elements.get(0))
    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
    else None
  }

  implicit class TraversalGraphOps[G](val steps: TraversalGraph[G]) {
    type EndDomain = steps.EndDomain
    def raw: GremlinScala[G] = steps.raw.asInstanceOf[GremlinScala[G]]

    def collect[A: ClassTag](pf: PartialFunction[G, A]): Traversal[A, A] = new Traversal(raw.collect(pf), UniMapping.identity)

    def groupBy[K](k: GremlinScala[K]): Traversal[JMap[K, JCollection[G]], JMap[K, JCollection[G]]] =
      new Traversal(raw.group(By(k)), UniMapping.identity)

    def groupBy[K, V](k: By[K], v: By[V]): Traversal[JMap[K, JCollection[V]], JMap[K, JCollection[V]]] =
      new Traversal(raw.group(k, v), UniMapping.identity)

    def fold: Traversal[JList[G], JList[G]] = new Traversal(raw.fold, UniMapping.identity)

    def outTo[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] =
      Traversal(raw.out(ru.typeOf[E].typeSymbol.name.toString))

    def outToE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge] =
      Traversal(raw.outE(ru.typeOf[E].typeSymbol.name.toString))

    def outE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge] =
      Traversal(raw.outE())

    def inTo[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] =
      Traversal(raw.in(ru.typeOf[E].typeSymbol.name.toString))
    def inToE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge] = Traversal(raw.inE(ru.typeOf[E].typeSymbol.name.toString))

    def inV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex]  = Traversal(raw.inV())
    def outV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex] = Traversal(raw.outV())

    def in()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex]  = Traversal(raw.in())
    def out()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] = Traversal(raw.out())

    def otherV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex] = Traversal(raw.otherV())

    def label(implicit ev: G <:< Element): Traversal[String, String]                 = new Traversal(raw.label(), UniMapping.string)
    def value[A: ClassTag](key: Key[A])(implicit ev: G <:< Element): Traversal[A, A] = new Traversal[A, A](raw.value(key), UniMapping.identity)

    def _id(implicit ev: G <:< Element) = new Traversal[String, AnyRef](raw.id(), IdMapping)

    def property[DD, GG](name: String, mapping: Mapping[_, DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG] =
      new Traversal[DD, GG](raw.values[GG](name), mapping)
    def _createdBy(implicit ev: G <:< Element): Traversal[String, String] = property("_createdBy", UniMapping.string)
    def _createdAt(implicit ev: G <:< Element): Traversal[Date, Date]     = property("_createdAt", UniMapping.date)
    def _updatedBy(implicit ev: G <:< Element): Traversal[String, String] = property("_updatedBy", UniMapping.string)
    def _updatedAt(implicit ev: G <:< Element): Traversal[Date, Date]     = property("_updatedAt", UniMapping.date)
  }

  implicit class VertexStepsOps[E <: Product](val steps: VertexSteps[E]) {

    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[Seq[E with Entity]] =
      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)

    def updateOne(fields: (String, Any)*)(implicit authContext: AuthContext): Try[E with Entity] =
      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext).flatMap {
        case e if e.size == 1 => Success(e.head)
        case e if e.isEmpty   => Failure(NotFoundError(s"${steps.model.label} not found"))
        case _                => Failure(InternalError("Multiple entities present while only one is expected"))
      }
  }

  implicit class EdgeStepsOps[E <: Product](val steps: EdgeSteps[E, _, _]) {

    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[Seq[E with Entity]] =
      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)
  }

  implicit class ElementStepsOps[S <: BaseElementSteps](val steps: S) {
    private def newInstance0(newRaw: GremlinScala[steps.EndGraph]): S = steps.newInstance(newRaw).asInstanceOf[S]
    def raw: GremlinScala[steps.EndGraph]                             = steps.raw
    def getByIds(ids: String*): S                                     = newInstance0(raw.hasId(ids: _*))

    def get(vertex: Vertex): S = newInstance0(raw.hasId(vertex.id()))

    def get(entity: Entity): S = newInstance0(raw.hasId(entity._id))
  }

  implicit class BaseTraversalOps[T <: BaseTraversal](val traversal: T) {

    def raw: GremlinScala[traversal.EndGraph] = traversal.raw

    def converter: Converter.Aux[traversal.EndDomain, traversal.EndGraph] = traversal.converter

    def start(): T = newInstance0(__[traversal.EndGraph])

    private def newInstance0(newRaw: GremlinScala[traversal.EndGraph]): T = traversal.newInstance(newRaw).asInstanceOf[T]

    def range(from: Long, to: Long): T =
      if (from == 0 && to == Long.MaxValue) traversal
      else newInstance0(traversal.raw.range(from, to))

    def richPage[DD: Renderer](from: Long, to: Long, withTotal: Boolean)(f: T => TraversalLike[DD, _]): PagedResult[DD] = {
      logger.debug(s"Execution of $raw (richPage)")
      val size   = if (withTotal) Some(newInstance0(raw.clone).count) else None
      val values = f(traversal.range(from, to))
      PagedResult(values, size)
    }

    def exists(): Boolean = {
      logger.debug(s"Execution of $raw (exists)")
      raw.limit(1).traversal.hasNext
    }

    def existsOrFail(): Try[Unit] = if (exists()) Success(()) else Failure(AuthorizationError("Unauthorized action"))

    def getCount: Long = raw.count().head()

    def count: Traversal[Long, JLong] = new Traversal(raw.count(), UniMapping.jlong)

    def sort(orderBys: OrderBy[_]*): T = newInstance0(raw.order(orderBys: _*))

    def filter(f: T => BaseTraversal): T = newInstance0(raw.filter(g => f(newInstance0(g)).raw))

    def filterNot(f: T => BaseTraversal): T = newInstance0(raw.filterNot(g => f(newInstance0(g)).raw))

    def coalesce[A: ClassTag](f: (T => Traversal[_, A])*): Traversal[A, A] = {
      val ff = f.map(t => (g: GremlinScala[traversal.EndGraph]) => t(newInstance0(g)).raw)
      new Traversal[A, A](raw.coalesce(ff: _*), UniMapping.identity)
    }

    def coalesce[G, D](mapping: Mapping[_, D, G])(f: (T => Traversal[D, G])*): Traversal[D, G] = {
      val ff = f.map(t => (g: GremlinScala[traversal.EndGraph]) => t(newInstance0(g)).raw)
      new Traversal[D, G](raw.coalesce(ff: _*), mapping)
    }

    def unfold[A: ClassTag]: Traversal[A, A] = new Traversal(raw.unfold[A](), UniMapping.identity)

    def project[A <: Product: ClassTag](builder: ProjectionBuilder[Nil.type] => ProjectionBuilder[A]): Traversal[A, A] =
      new Traversal(raw.project(builder), UniMapping.identity)

    def project[A](bys: By[_ <: A]*): Traversal[JCollection[A], JCollection[A]] = {
      val labels = bys.map(_ => UUID.randomUUID().toString)
      val traversal: GremlinScala[JMap[String, A]] =
        bys.foldLeft(raw.project[A](labels.head, labels.tail: _*))((t, b) => GremlinScala(b.apply(t.traversal)))
      new Traversal(traversal.selectValues, UniMapping.identity)
    }

    // TODO check if A = EndGraph
    def as[A](stepLabel: StepLabel[A]): T = newInstance0(new GremlinScala[traversal.EndGraph](raw.traversal.as(stepLabel.name)))

    def order(orderBys: List[OrderBy[_]]): T = newInstance0(raw.order(orderBys: _*))

    def remove(): Unit = {
      logger.debug(s"Execution of $raw (drop)")
      raw.drop().iterate()
      ()
    }

    def group[ModulatedKeys, ModulatedValues](
        keysBy: By[ModulatedKeys],
        valuesBy: By[ModulatedValues]
    ): Traversal[JMap[ModulatedKeys, JCollection[ModulatedValues]], JMap[ModulatedKeys, JCollection[ModulatedValues]]] =
      new Traversal(raw.group(keysBy, valuesBy), UniMapping.identity)

    def groupCount[Modulated](by: By[Modulated]): Traversal[JMap[Modulated, JLong], JMap[Modulated, JLong]] =
      new Traversal(raw.groupCount(by), UniMapping.identity)

    def sack[A: ClassTag](): Traversal[A, A] = new Traversal[A, A](raw.sack(), UniMapping.identity)

    def sack[SackType, Modulated](func: (SackType, Modulated) => SackType, by: By[Modulated]): T =
      newInstance0(raw.sack[SackType, Modulated](func, by))

    def constant[A: ClassTag](value: A): Traversal[A, A] = new Traversal(raw.constant(value), UniMapping.identity)

    def or(f: (T => BaseTraversal)*): T = {
      val filters = f.map(r => (g: GremlinScala[traversal.EndGraph]) => r(newInstance0(g)).raw)
      newInstance0(raw.or(filters: _*))
    }

    def and(f: (T => BaseTraversal)*): T = {
      val filters = f.map(r => (g: GremlinScala[traversal.EndGraph]) => r(newInstance0(g)).raw)
      newInstance0(raw.and(filters: _*))
    }

    def not(t: T => BaseTraversal): BaseTraversal =
      newInstance0(raw.not((g: GremlinScala[traversal.EndGraph]) => t(newInstance0(g)).raw))

    def choose[A: ClassTag](
        predicate: T => BaseTraversal,
        onTrue: T => TraversalLike[A, A],
        onFalse: T => TraversalLike[A, A]
    ): Traversal[A, A] =
      new Traversal[A, A](
        raw.choose(
          (g: GremlinScala[traversal.EndGraph]) => predicate(newInstance0(g)).raw,
          (g: GremlinScala.Aux[traversal.EndGraph, HNil]) => onTrue(newInstance0(g)).raw,
          (g: GremlinScala.Aux[traversal.EndGraph, HNil]) => onFalse(newInstance0(g)).raw
        ),
        UniMapping.identity[A]
      )

    def choose[A, B: ClassTag](on: T => TraversalLike[_, A], options: BranchOption[T, TraversalLike[B, B]]*): Traversal[B, B] = {
      val jTraversal = options.foldLeft[GraphTraversal[_, B]](raw.traversal.choose(on(this.start()).raw.traversal)) { (tr, option) =>
        tr.option(option.pickToken, option.traversal(this.start()).raw.traversal)
      }
      Traversal[B](GremlinScala(jTraversal))
    }

    def where(predicate: P[String])(implicit ev: traversal.EndGraph <:< Element): T             = newInstance0(raw.where(predicate))
    def has[A](key: String, value: A)(implicit ev: traversal.EndGraph <:< Element): T           = newInstance0(raw.has(Key[A](key), P.eq(value)))
    def has[A](key: String, predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T    = newInstance0(raw.has(Key[A](key), predicate))
    def has[A](key: String)(implicit ev: traversal.EndGraph <:< Element): T                     = newInstance0(raw.has(Key[A](key)))
    def hasNot[A](key: String, predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasNot(Key[A](key), predicate))
    def hasNot[A](key: String, value: A)(implicit ev: traversal.EndGraph <:< Element): T        = newInstance0(raw.hasNot(Key[A](key), P.eq(value)))
    def hasNot[A](key: String): T                                                               = newInstance0(raw.hasNot(Key[A](key)))

    def hasId(ids: String*)(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasId(ids: _*))
  }

  implicit class TraversalOps[D, G](val traversal: TraversalLike[D, G]) {
    def raw: GremlinScala[G] = traversal.raw

    def converter: Converter.Aux[D, G] = traversal.converter

    private def newInstance0(newRaw: GremlinScala[G]): TraversalLike[D, G] = traversal.newInstance(newRaw) //.asInstanceOf[Traversal[D, G]]

    def toList: List[D] = {
      logger.debug(s"Execution of $raw (toList)")
      raw.toList.map(converter.toDomain)
    }

    def toIterator: Iterator[D] = {
      logger.debug(s"Execution of $raw (toIterator)")
      raw.traversal.asScala.map(converter.toDomain)
    }

    def page(from: Long, to: Long, withTotal: Boolean)(implicit renderer: Renderer[D]): PagedResult[D] = {
      logger.debug(s"Execution of $raw (page)")
      val size = if (withTotal) Some(newInstance0(raw.clone).count) else None
      val r    = traversal.range(from, to)
      PagedResult(r, size)
    }

    def head(): D = {
      logger.debug(s"Execution of $raw (head)")
      converter.toDomain(raw.head)
    }

    def headOption(): Option[D] = {
      logger.debug(s"Execution of $raw (headOption)")
      raw.headOption().map(converter.toDomain)
    }

    def getOrFail(): Try[D] =
      headOption()
        .fold[Try[D]](Failure(NotFoundError(s"${traversal.typeName} not found")))(Success.apply)

    def orFail(ex: Exception): Try[D] = headOption().fold[Try[D]](Failure(ex))(Success.apply)

    def sum[N <: Number: ClassTag]()(implicit toNumber: G => N): Traversal[N, N] =
      new Traversal(raw.sum[N]()(toNumber), UniMapping.identity)

    def min[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      new Traversal(raw.min[C]()(toComparable), UniMapping.identity)

    def max[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      new Traversal(raw.max[C]()(toComparable), UniMapping.identity)

    def mean[N <: Number]()(implicit toNumber: G => N): Traversal[Double, JDouble] = new Traversal(raw.mean, UniMapping.jdouble)

    def is(predicate: P[G]): TraversalLike[D, G] = newInstance0(raw.is(predicate))

    def map[A: ClassTag](f: D => A): Traversal[A, A] =
      new Traversal[A, A](raw.map(x => f(converter.toDomain(x))), UniMapping.identity)
  }
}
