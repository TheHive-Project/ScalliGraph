package org.thp.scalligraph.steps

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, UUID, Collection => JCollection, List => JList, Map => JMap}

import gremlin.scala.{ColumnType, Key, P, StepLabel}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, GraphTraversal}
import org.apache.tinkerpop.gremlin.process.traversal.{Order, Scope, Traverser}
import org.apache.tinkerpop.gremlin.structure.Column
import org.thp.scalligraph.models.{Mapping, UniMapping}
import play.api.Logger
import shapeless.ops.tuple.{Prepend => TuplePrepend}
import shapeless.syntax.std.tuple._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object StepsOps {
  lazy val logger: Logger = Logger(getClass)

  implicit class UntypedTraversalOps(untypedTraversal: UntypedTraversal) {
    implicit class UntypedGraphTraversalOps(graphTraversal: GraphTraversal[_, _]) {
      def untyped: GraphTraversal[Any, Any] = graphTraversal.asInstanceOf[GraphTraversal[Any, Any]]
    }

    def typed[D, G: ClassTag]: Traversal[D, G] =
      new Traversal[D, G](new gremlin.scala.GremlinScala(untypedTraversal.traversal.asInstanceOf[GraphTraversal[_, G]]), _ => ???)
    def min: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Any](_.min[Comparable[Any]], identity)
    def max: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Any](_.max[Comparable[Any]], identity)
    def sum: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Number](_.sum[Number], identity)
    def mean: UntypedTraversal       = untypedTraversal.onGraphTraversal[Any, Number](_.mean[Number], identity)
    def count: UntypedTraversal      = untypedTraversal.onGraphTraversal[Any, Any](_.count(), _ => GraphConverter.long)
    def getCount: Long               = untypedTraversal.traversal.count().next()
    def localCount: UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.count(Scope.local), _ => GraphConverter.long)
    def as(label: String, convMap: GraphConverter[_, _] => GraphConverter[_, _] = GraphConverter.identity): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.as(label), convMap)
    def select(label: String, convMap: GraphConverter[_, _] => GraphConverter[_, _] = GraphConverter.identity): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.select(label), convMap)
    def fold: UntypedTraversal                 = untypedTraversal.onGraphTraversal[Any, Any](_.fold, GraphConverter.list)
    def unfold: UntypedTraversal               = untypedTraversal.onGraphTraversal[Any, Any](_.unfold[Any])
    def selectKeys: UntypedTraversal           = untypedTraversal.onGraphTraversal[Any, Any](_.select(Column.keys))
    def selectValues: UntypedTraversal         = untypedTraversal.onGraphTraversal[Any, Any](_.select(Column.values))
    def limit(max: Long): UntypedTraversal     = untypedTraversal.onGraphTraversal[Any, Any](_.limit(max))
    def map[F, T](f: F => T): UntypedTraversal = untypedTraversal.onGraphTraversal[Any, F](_.map((t: Traverser[F]) => f(t.get)))
    def getByIds(ids: String*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](t => ids.headOption.fold(t.limit(0))(t.hasId(_, ids.tail: _*)))
    def constant[A](cst: A): UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.constant(cst))
    def group(k: UntypedBySelector => UntypedByResult, v: UntypedBySelector => UntypedByResult): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](t => v(bySelector)(k(bySelector)(t.group())))
    def sort(f: (UntypedSortBySelector => UntypedByResult)*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](t => f.map(_(sortBySelector)).foldLeft(t.order)((acc, s) => s(acc).untyped))
    def project(f: (UntypedBySelector => UntypedByResult)*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](t => f.map(_(bySelector)).foldLeft(t.group.untyped)((acc, p) => p(acc).untyped))
    def filter(f: UntypedTraversal => UntypedTraversal): UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.filter(__.start))
    def is(predicate: P[_]): UntypedTraversal                             = untypedTraversal.onGraphTraversal[Any, Any](_.is(predicate))
    def or(f: (UntypedTraversal => UntypedTraversal)*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.or(f.map(_(UntypedTraversal.start).traversal): _*))
    def and(f: (UntypedTraversal => UntypedTraversal)*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.and(f.map(_(UntypedTraversal.start).traversal): _*))
    def not(t: UntypedTraversal => UntypedTraversal): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.not(t(UntypedTraversal.start).traversal))
    def coalesce(f: (UntypedTraversal => UntypedTraversal)*): UntypedTraversal =
      untypedTraversal.onGraphTraversal[Any, Any](_.coalesce(f.map(_(UntypedTraversal.start).traversal): _*))
    def property[DD, GG](name: String, mapping: Mapping[_, DD, GG]): Traversal[DD, GG] =
      untypedTraversal.onGraphTraversal(_.values(), mapping.toDomain(_))
    def _createdBy: Traversal[String, String] = property("_createdBy", UniMapping.string)
    def _createdAt: Traversal[Date, Date]     = property("_createdAt", UniMapping.date)
    def _updatedBy: Traversal[String, String] = property("_updatedBy", UniMapping.string)
    def _updatedAt: Traversal[Date, Date]     = property("_updatedAt", UniMapping.date)

    private def bySelector: UntypedBySelector         = new UntypedBySelector
    private def sortBySelector: UntypedSortBySelector = new UntypedSortBySelector
  }

  implicit class TraversalOps[D, G](traversal: Traversal[D, G]) {
    import gremlin.scala.{GremlinScala, StepLabel}

    def raw: GremlinScala[G]          = traversal.raw
    def deepRaw: GraphTraversal[_, G] = traversal.raw.traversal
    def toDomain(g: G): D             = traversal.toDomain(g)
    def untyped: UntypedTraversal     = new UntypedTraversal(deepRaw)

    def getCount: Long = {
      logger.debug(s"Execution of $raw (count)")
      raw.count().head()
    }

    def head(): D = {
      logger.debug(s"Execution of $raw (head)")
      toDomain(raw.head)
    }

    def headOption(): Option[D] = {
      logger.debug(s"Execution of $raw (headOption)")
      raw.headOption().map(toDomain)
    }

    def toList: List[D] = {
      logger.debug(s"Execution of $raw (toList)")
      raw.toList.map(toDomain)
    }

    def limit(max: Long): Traversal[D, G] = traversal.onRaw(_.limit(max))

    def range(low: Long, high: Long): Traversal[D, G] = traversal.onRaw(_.range(low, high))

    def map[A: ClassTag](f: D => A): Traversal[A, A] = traversal.onRawMap[A, A](_.map(x => f(toDomain(x))), identity)

    def count: Traversal[Long, JLong] = traversal.onRawMap[Long, JLong](_.count(), _.longValue)

    def localCount: Traversal[Long, JLong] = traversal.onRawMap[Long, JLong](_.count(Scope.local), _.longValue)

    def sum[N <: Number: ClassTag]()(implicit toNumber: G => N): Traversal[N, N] =
      traversal.onRawMap[N, N](_.sum[N]()(toNumber), identity)

    def min[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      traversal.onRawMap[C, C](_.min[C](), identity)

    def max[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      new Traversal(raw.max[C]()(toComparable), _ => ???)

    def mean[N <: Number]()(implicit toNumber: G => N): Traversal[Double, JDouble] =
      traversal.onRawMap(_.mean[N], _.doubleValue())

    def as[A](stepLabel: StepLabel[A]): Traversal[D, G] = traversal.onDeepRaw(_.as(stepLabel.name))

    def group[K, V](
        keysBy: GroupBySelector[D, G] => ByResult[G, V],
        valuesBy: GroupBySelector[D, G] => ByResult[G, K]
    ): Traversal[JMap[K, JCollection[V]], JMap[K, JCollection[V]]] =
      traversal.onDeepRawMap(
        ((_: GraphTraversal[_, G])
          .group
          .asInstanceOf[GraphTraversal[_, G]])
          .andThen(keysBy(groupBySelector))
          .andThen(_.asInstanceOf[GraphTraversal[_, G]])
          .andThen(valuesBy(groupBySelector))
          .andThen(_.asInstanceOf[GraphTraversal[_, JMap[K, JCollection[V]]]]),
        _ => identity[JMap[K, JCollection[V]]]
      )

    def select[A: ClassTag](label: StepLabel[A]): Traversal[A, A] =
      traversal.onDeepRawMap[A, A](_.select(label.name).asInstanceOf[GraphTraversal[_, A]], _ => identity[A])

    def fold: Traversal[Seq[D], JList[G]] = traversal.onDeepRawMap[Seq[D], JList[G]](_.fold(), c => listG => listG.asScala.map(c))

    def unfold[A: ClassTag]: Traversal[A, A] = traversal.onDeepRawMap(_.unfold[A](), _ => identity)

    def sort(f: (SortBySelector[D, G] => ByResult[G, G])*): Traversal[D, G] =
      traversal.onDeepRaw(t => f.map(_(sortBySelector)).foldLeft(t.order())((s, g) => g.app(s)))

    /* select only the keys from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectKeys[K: ClassTag](implicit columnType: ColumnType.Aux[G, K, _]): Traversal[K, K] =
      traversal.onDeepRawMap[K, K](_.select(Column.keys).asInstanceOf[GraphTraversal[K, K]], _ => identity)

    /* select only the values from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectValues[V: ClassTag](implicit columnType: ColumnType.Aux[G, _, V]): Traversal[V, V] =
      traversal.onDeepRawMap[V, V](_.select(Column.values).asInstanceOf[GraphTraversal[V, V]], _ => identity)

    def coalesce[DD, GG: ClassTag](f: (Traversal[D, G] => Traversal[DD, GG])*)(c: GG => DD): Traversal[DD, GG] =
      traversal.onDeepRawMap[DD, GG](_.coalesce(f.map(_(traversal.start).deepRaw): _*), _ => c)

    def project[A <: Product: ClassTag](
        builder: ProjectionBuilder[Nil.type, D, G] => ProjectionBuilder[A, D, G]
    ): Traversal[A, A] =
      traversal.onDeepRawMap(builder(projectionBuilder).build, _ => identity)

    //    def filter(f: T => BaseTraversal): T = newInstance0(raw.filter(g => f(newInstance0(g)).raw))

    private def projectionBuilder                          = new ProjectionBuilder[Nil.type, D, G](traversal, Nil, scala.Predef.identity, _ => Nil)
    private def genericBySelector: GenericBySelector[D, G] = new GenericBySelector[D, G](traversal)
    private def groupBySelector: GroupBySelector[D, G]     = new GroupBySelector[D, G](traversal)
    private def sortBySelector: SortBySelector[D, G]       = new SortBySelector[D, G](traversal)
  }
}

class ProjectionBuilder[E <: Product, D, G](
    traversal: Traversal[D, G],
    labels: Seq[String],
    addBy: GraphTraversal[_, JMap[String, Any]] => GraphTraversal[_, JMap[String, Any]],
    buildResult: JMap[String, Any] => E
) {
  //  def apply[U, TR <: Product](by: By[U])(implicit prepend: TuplePrepend.Aux[E, Tuple1[U], TR]): ProjectionBuilder[TR, T] = {
  //    val label = UUID.randomUUID().toString
  //    new ProjectionBuilder[TR, T](traversal, labels :+ label, addBy.andThen(by.apply), map => buildResult(map) :+ map.get(label).asInstanceOf[U])
  //  }

  def by[TR <: Product](implicit prepend: TuplePrepend.Aux[E, Tuple1[G], TR]): ProjectionBuilder[TR, D, G] = {
    val label = UUID.randomUUID().toString
    new ProjectionBuilder[TR, D, G](
      traversal,
      labels :+ label,
      addBy.andThen(_.by),
      map => buildResult(map) :+ map.get(label).asInstanceOf[G]
    )
  }

  def by[U, TR <: Product](key: Key[U])(implicit prepend: TuplePrepend.Aux[E, Tuple1[U], TR]): ProjectionBuilder[TR, D, G] = {
    val label = UUID.randomUUID().toString
    new ProjectionBuilder[TR, D, G](
      traversal,
      labels :+ label,
      addBy.andThen(_.by(key.name)),
      map => buildResult(map) :+ map.get(label).asInstanceOf[U]
    )
  }

  def by[U, TR <: Product](
      f: Traversal[D, G] => Traversal[_, U]
  )(implicit prepend: TuplePrepend.Aux[E, Tuple1[U], TR]): ProjectionBuilder[TR, D, G] = {
    val label = UUID.randomUUID().toString
    new ProjectionBuilder[TR, D, G](
      traversal,
      labels :+ label,
      addBy.andThen(_.by(f(traversal.start).raw.traversal)),
      map => buildResult(map) :+ map.get(label).asInstanceOf[U]
    )
  }

//  private[steps] def build(g: GremlinScala[_]): GremlinScala[E] =
//    GremlinScala(addBy(g.traversal.project(labels.head, labels.tail: _*))).map(buildResult)
  private[steps] def build(g: GraphTraversal[_, _]): GraphTraversal[_, E] =
    addBy(g.project(labels.head, labels.tail: _*)).map[E]((t: Traverser[JMap[String, Any]]) => buildResult(t.get))

}

class GroupBySelector[D, G](origin: => Traversal[D, G]) {
  def by: ByResult[G, G] = (_: GraphTraversal[_, G]).by()
  def byLabel[B](stepLabel: StepLabel[B]): ByResult[G, B] =
    (t: GraphTraversal[_, G]) => t.by(stepLabel.name).asInstanceOf[GraphTraversal[_, B]]
  def by[B](f: Traversal[D, G] => Traversal[_, B]): ByResult[G, B] = new ByResult[G, B] {
    override def apply(t: GraphTraversal[_, G]): GraphTraversal[_, B] = t.by(f(origin.start).deepRaw).asInstanceOf[GraphTraversal[_, B]]
  }
}

class SortBySelector[D, G](origin: => Traversal[D, G]) {
//  def by[B](f: Traversal[D, G] => Traversal[_, B]): ByResult[G, G]               = (_: GraphTraversal[_, G]).by(f(origin.start).deepRaw)
  def by[B](f: Traversal[D, G] => Traversal[_, B], order: Order): ByResult[G, G] = (_: GraphTraversal[_, G]).by(f(origin.start).deepRaw, order)
}

class GenericBySelector[D, G](origin: => Traversal[D, G]) {
  def by: ByResult[G, G]                                           = (_: GraphTraversal[_, G]).by
  def by[B](f: Traversal[D, G] => Traversal[_, _]): ByResult[G, G] = (_: GraphTraversal[_, G]).by(f(origin.start).deepRaw)
}

abstract class ByResult[F, T] extends (GraphTraversal[_, F] => GraphTraversal[_, T]) {
  def app[A](t: GraphTraversal[A, F]): GraphTraversal[A, T] = apply(t).asInstanceOf[GraphTraversal[A, T]]
}

class UntypedBySelector {
  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

  def by: UntypedByResult                                          = (_: GraphTraversal[_, _]).by()
  def by(f: UntypedTraversal => UntypedTraversal): UntypedByResult = (_: GraphTraversal[_, _]).by(f(new UntypedTraversal(__.start())).traversal)
}

class UntypedSortBySelector {
  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
  def by(f: UntypedTraversal => UntypedTraversal, order: Order): UntypedByResult =
    (_: GraphTraversal[_, _]).by(f(new UntypedTraversal(__.start())).traversal, order)
}
abstract class UntypedByResult extends (GraphTraversal[_, _] => GraphTraversal[_, _])

//
//object GroupBySelectorResult {
//  def apply(f: )
//}
//
//  private lazy val logger: Logger = Logger(classOf[Traversal[_, _]])
//
//  def union[T <: VertexSteps[_]](
//      srv: VertexSrv[_, T]
//  )(firstTraversal: GremlinScala[Vertex] => T, otherTraverals: (GremlinScala[Vertex] => T)*)(implicit graph: Graph): T = {
//    val traversals = (firstTraversal +: otherTraverals)
//      .map(t => (g: GremlinScala.Aux[Int, HNil]) => t(GremlinScala[Vertex, HNil](g.traversal.V())).raw)
//    srv.steps(graph.inject(1).unionFlat(traversals: _*))
//  }
//
//  def onlyOneOf[A](elements: JList[A]): A = {
//    val size = elements.size
//    if (size == 1) elements.get(0)
//    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
//    else throw InternalError("No element found")
//  }
//
//  def atMostOneOf[A](elements: JList[A]): Option[A] = {
//    val size = elements.size
//    if (size == 1) Some(elements.get(0))
//    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
//    else None
//  }
//
//  implicit class TraversalGraphOps[G](val steps: TraversalGraph[G]) {
//    type EndDomain = steps.EndDomain
//    def raw: GremlinScala[G] = steps.raw.asInstanceOf[GremlinScala[G]]
//
//    def collect[A: ClassTag](pf: PartialFunction[G, A]): Traversal[A, A] = new Traversal(raw.collect(pf), UniMapping.identity)
//
//    def groupBy[K](k: GremlinScala[K]): Traversal[JMap[K, JCollection[G]], JMap[K, JCollection[G]]] =
//      new Traversal(raw.group(By(k)), UniMapping.identity)
//
//    def groupBy[K, V](k: By[K], v: By[V]): Traversal[JMap[K, JCollection[V]], JMap[K, JCollection[V]]] =
//      new Traversal(raw.group(k, v), UniMapping.identity)
//
//    def fold: Traversal[JList[G], JList[G]] = new Traversal(raw.fold, UniMapping.identity)
//
//    def outTo[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] =
//      Traversal(raw.out(ru.typeOf[E].typeSymbol.name.toString))
//
//    def outToE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge] =
//      Traversal(raw.outE(ru.typeOf[E].typeSymbol.name.toString))
//
//    def outE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge] =
//      Traversal(raw.outE())
//
//    def inTo[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] =
//      Traversal(raw.in(ru.typeOf[E].typeSymbol.name.toString))
//    def inToE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge] = Traversal(raw.inE(ru.typeOf[E].typeSymbol.name.toString))
//
//    def inV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex]  = Traversal(raw.inV())
//    def outV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex] = Traversal(raw.outV())
//
//    def in()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex]  = Traversal(raw.in())
//    def out()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex] = Traversal(raw.out())
//
//    def otherV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex] = Traversal(raw.otherV())
//
//    def label(implicit ev: G <:< Element): Traversal[String, String]                  = new Traversal(raw.label(), UniMapping.string)
//    def value[A: ClassTag](name: String)(implicit ev: G <:< Element): Traversal[A, A] = new Traversal[A, A](raw.value[A](name), UniMapping.identity)
//
//    def _id(implicit ev: G <:< Element) = new Traversal[String, AnyRef](raw.id(), IdMapping)
//
//    def property[DD, GG](name: String, mapping: Mapping[_, DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG] =
//      new Traversal[DD, GG](raw.values[GG](name), mapping)
//    def _createdBy(implicit ev: G <:< Element): Traversal[String, String] = property("_createdBy", UniMapping.string)
//    def _createdAt(implicit ev: G <:< Element): Traversal[Date, Date]     = property("_createdAt", UniMapping.date)
//    def _updatedBy(implicit ev: G <:< Element): Traversal[String, String] = property("_updatedBy", UniMapping.string)
//    def _updatedAt(implicit ev: G <:< Element): Traversal[Date, Date]     = property("_updatedAt", UniMapping.date)
//  }
//
//  implicit class VertexStepsOps[E <: Product](val steps: VertexSteps[E]) {
//
//    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[Seq[E with Entity]] =
//      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)
//
//    def updateOne(fields: (String, Any)*)(implicit authContext: AuthContext): Try[E with Entity] =
//      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext).flatMap {
//        case e if e.size == 1 => Success(e.head)
//        case e if e.isEmpty   => Failure(NotFoundError(s"${steps.model.label} not found"))
//        case _                => Failure(InternalError("Multiple entities present while only one is expected"))
//      }
//  }
//
//  implicit class EdgeStepsOps[E <: Product](val steps: EdgeSteps[E, _, _]) {
//
//    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[Seq[E with Entity]] =
//      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)
//  }
//
//  implicit class ElementStepsOps[S <: BaseElementSteps](val steps: S) {
//    private def newInstance0(newRaw: GremlinScala[steps.EndGraph]): S = steps.newInstance(newRaw).asInstanceOf[S]
//    def raw: GremlinScala[steps.EndGraph]                             = steps.raw
//    def getByIds(ids: String*): S                                     = newInstance0(raw.hasId(ids: _*))
//
//    def get(vertex: Vertex): S = newInstance0(raw.hasId(vertex.id()))
//
//    def get(entity: Entity): S = newInstance0(raw.hasId(entity._id))
//  }
//
//  implicit class BaseTraversalOps[T <: BaseTraversal](val traversal: T) {
//
//    def raw: GremlinScala[traversal.EndGraph] = traversal.raw
//
//    def converter: Converter.Aux[traversal.EndDomain, traversal.EndGraph] = traversal.converter
//
//    def start: T = newInstance0(__[traversal.EndGraph])
//
//    private def newInstance0(newRaw: GremlinScala[traversal.EndGraph]): T = traversal.newInstance(newRaw).asInstanceOf[T]
//
//    def range(from: Long, to: Long): T =
//      if (from == 0 && to == Long.MaxValue) traversal
//      else newInstance0(traversal.raw.range(from, to))
//
//    def limit(n: Long): T = newInstance0(traversal.raw.limit(n))
//
//    def richPage[DD: Renderer](from: Long, to: Long, withTotal: Boolean)(f: T => TraversalLike[DD, _]): PagedResult[DD] = {
//      logger.debug(s"Execution of $raw (richPage)")
//      val size   = if (withTotal) Some(newInstance0(raw.clone).count) else None
//      val values = f(traversal.range(from, to))
//      PagedResult(values, size)
//    }
//
//    def exists(): Boolean = {
//      logger.debug(s"Execution of $raw (exists)")
//      raw.limit(1).traversal.hasNext
//    }
//
//    def existsOrFail(): Try[Unit] = if (exists()) Success(()) else Failure(AuthorizationError("Unauthorized action"))
//
//    def dedup: T = newInstance0(raw.dedup())
//

//
//
//    def sort(orderBys: OrderBy[_]*): T = newInstance0(raw.order(orderBys: _*))
//

//
//    def filterNot(f: T => BaseTraversal): T = newInstance0(raw.filterNot(g => f(newInstance0(g)).raw))
//
//    def coalesce[A: ClassTag](f: (T => Traversal[_, A])*): Traversal[A, A] = {
//      val ff = f.map(t => (g: GremlinScala[traversal.EndGraph]) => t(newInstance0(g)).raw)
//      new Traversal[A, A](raw.coalesce(ff: _*), UniMapping.identity)
//    }
//
//    def coalesce[G, D](mapping: Mapping[_, D, G])(f: (T => Traversal[D, G])*): Traversal[D, G] = {
//      val ff = f.map(t => (g: GremlinScala[traversal.EndGraph]) => t(newInstance0(g)).raw)
//      new Traversal[D, G](raw.coalesce(ff: _*), mapping)
//    }
//
//
////    def project[A <: Product: ClassTag](builder: GremlinProjectionBuilder[Nil.type] => GremlinProjectionBuilder[A]): Traversal[A, A] =
////      new Traversal(raw.project(builder), UniMapping.identity)
//
//    def project[A](bys: By[_ <: A]*): Traversal[JCollection[A], JCollection[A]] = {
//      val labels = bys.map(_ => UUID.randomUUID().toString)
//      val traversal: GremlinScala[JMap[String, A]] =
//        bys.foldLeft(raw.project[A](labels.head, labels.tail: _*))((t, b) => GremlinScala(b.apply(t.traversal)))
//      new Traversal(traversal.selectValues, UniMapping.identity)
//    }
//
//
//    // TODO check if A = EndGraph
//
//    def order(orderBys: List[OrderBy[_]]): T = newInstance0(raw.order(orderBys: _*))
//
//    def remove(): Unit = {
//      logger.debug(s"Execution of $raw (drop)")
//      raw.drop().iterate()
//      ()
//    }
//
//
//    def groupCount[Modulated](by: By[Modulated]): Traversal[JMap[Modulated, JLong], JMap[Modulated, JLong]] =
//      new Traversal(raw.groupCount(by), UniMapping.identity)
//
//    def sack[A: ClassTag](): Traversal[A, A] = new Traversal[A, A](raw.sack(), UniMapping.identity)
//
//    def sack[SackType, Modulated](func: (SackType, Modulated) => SackType, by: By[Modulated]): T =
//      newInstance0(raw.sack[SackType, Modulated](func, by))
//
//    def constant[A: ClassTag](value: A): Traversal[A, A] = new Traversal(raw.constant(value), UniMapping.identity)
//
//
//    def choose[A: ClassTag](
//        predicate: T => BaseTraversal,
//        onTrue: T => TraversalLike[A, A],
//        onFalse: T => TraversalLike[A, A]
//    ): Traversal[A, A] =
//      new Traversal[A, A](
//        raw.choose(
//          (g: GremlinScala[traversal.EndGraph]) => predicate(newInstance0(g)).raw,
//          (g: GremlinScala.Aux[traversal.EndGraph, HNil]) => onTrue(newInstance0(g)).raw,
//          (g: GremlinScala.Aux[traversal.EndGraph, HNil]) => onFalse(newInstance0(g)).raw
//        ),
//        UniMapping.identity[A]
//      )
//
//    def choose[A, B: ClassTag](on: T => TraversalLike[_, A], options: BranchOption[T, TraversalLike[B, B]]*): Traversal[B, B] = {
//      val jTraversal = options.foldLeft[GraphTraversal[_, B]](raw.traversal.choose(on(this.start).raw.traversal)) { (tr, option) =>
//        tr.option(option.pickToken, option.traversal(this.start).raw.traversal)
//      }
//      Traversal[B](GremlinScala(jTraversal))
//    }
//
//    def where(predicate: P[String])(implicit ev: traversal.EndGraph <:< Element): T             = newInstance0(raw.where(predicate))
//    def has[A](key: String, value: A)(implicit ev: traversal.EndGraph <:< Element): T           = newInstance0(raw.has(Key[A](key), P.eq(value)))
//    def has[A](key: String, predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T    = newInstance0(raw.has(Key[A](key), predicate))
//    def has[A](key: String)(implicit ev: traversal.EndGraph <:< Element): T                     = newInstance0(raw.has(Key[A](key)))
//    def hasNot[A](key: String, predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasNot(Key[A](key), predicate))
//    def hasNot[A](key: String, value: A)(implicit ev: traversal.EndGraph <:< Element): T        = newInstance0(raw.hasNot(Key[A](key), P.eq(value)))
//    def hasNot[A](key: String): T                                                               = newInstance0(raw.hasNot(Key[A](key)))
//
//    def hasId(ids: String*)(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasId(ids: _*))
//
//    def union[A: ClassTag](t: (T => TraversalGraph[A])*): TraversalGraph[A] =
//      Traversal[A](raw.unionFlat(t.map(_.compose(newInstance0).andThen(_.raw)): _*))
//  }
//
//  implicit class TraversalOps[D, G](val traversal: TraversalLike[D, G]) {
//    def raw: GremlinScala[G] = traversal.raw
//
//    def converter: Converter.Aux[D, G] = traversal.converter
//
//    private def newInstance0(newRaw: GremlinScala[G]): TraversalLike[D, G] = traversal.newInstance(newRaw) //.asInstanceOf[Traversal[D, G]]
//
//    def toIterator: Iterator[D] = {
//      logger.debug(s"Execution of $raw (toIterator)")
//      raw.traversal.asScala.map(converter.toDomain)
//    }
//
//    def page(from: Long, to: Long, withTotal: Boolean)(implicit renderer: Renderer[D]): PagedResult[D] = {
//      logger.debug(s"Execution of $raw (page)")
//      val size = if (withTotal) Some(newInstance0(raw.clone).count) else None
//      val r    = traversal.range(from, to)
//      PagedResult(r, size)
//    }
//
//
//
//    def getOrFail(): Try[D] = getOrFail(traversal.typeName)
//
//    def getOrFail(entityName: String): Try[D] =
//      headOption()
//        .fold[Try[D]](Failure(NotFoundError(s"$entityName not found")))(Success.apply)
//
//    def orFail(ex: Exception): Try[D] = headOption().fold[Try[D]](Failure(ex))(Success.apply)
//
//

//
//}

//trait BranchOption[T <: BaseTraversal, R <: BaseTraversal] {
//  def traversal: T => R
//  def pickToken: Any
//}
//
//case class BranchCase[BranchOn, T <: BaseTraversal, R <: BaseTraversal](pickToken: BranchOn, traversal: T => R) extends BranchOption[T, R]
//
//case class BranchMatchAll[T <: BaseTraversal, R <: BaseTraversal](traversal: T => R) extends BranchOption[T, R] {
//  override def pickToken = Pick.any
//}
//
///* if nothing else matched in branch/choose step */
//case class BranchOtherwise[T <: BaseTraversal, R <: BaseTraversal](traversal: T => R) extends BranchOption[T, R] {
//  override def pickToken = Pick.none
//}
//
