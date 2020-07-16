package org.thp.scalligraph.steps

import java.lang.{Double => JDouble, Long => JLong}
import java.util.{Date, UUID, Collection => JCollection, List => JList, Map => JMap}

import scala.reflect.runtime.{universe => ru}
import gremlin.scala.{ColumnType, Element, Key, P, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, GraphTraversal}
import org.apache.tinkerpop.gremlin.process.traversal.{Order, Scope, Traverser}
import org.apache.tinkerpop.gremlin.structure.Column
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.models.{Mapping, Model, SingleMapping, UniMapping}
import play.api.Logger
import shapeless.ops.tuple.{Prepend => TuplePrepend}
import shapeless.syntax.std.tuple._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object StepsOps {
  lazy val logger: Logger = Logger(getClass)

//  implicit class UntypedTraversalOps(untypedTraversal: UntypedTraversal) {
//    implicit class UntypedGraphTraversalOps(graphTraversal: GraphTraversal[_, _]) {
//      def untyped: GraphTraversal[Any, Any] = graphTraversal.asInstanceOf[GraphTraversal[Any, Any]]
//    }
//
//    def toIterator: Iterator[Any] = {
//      val iterator = untypedTraversal.traversal.asScala
//      if (untypedTraversal.converter.isIdentity) iterator
//      else iterator.map(untypedTraversal.converter.untypedApply)
//    }
//
//    def toSeq: Seq[Any] = toIterator.toSeq
//
//    def typed[D, G, C]: Traversal[D, G, C] =
//      new Traversal[D, G, C](
//        new gremlin.scala.GremlinScala(untypedTraversal.traversal.asInstanceOf[GraphTraversal[_, G]]),
//        untypedTraversal.converter.asInstanceOf[GraphConverter[D, G]]
//      )
//    def min: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Any](_.min[Comparable[Any]], identity)
//    def max: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Any](_.max[Comparable[Any]], identity)
//    def sum: UntypedTraversal        = untypedTraversal.onGraphTraversal[Any, Number](_.sum[Number], identity)
//    def mean: UntypedTraversal       = untypedTraversal.onGraphTraversal[Any, Number](_.mean[Number], identity)
//    def count: UntypedTraversal      = untypedTraversal.onGraphTraversal[Any, Any](_.count(), _ => GraphConverter.long)
//    def getCount: Long               = untypedTraversal.traversal.count().next()
//    def localCount: UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.count(Scope.local), _ => GraphConverter.long)
//
//    def as(label: String)(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.as(label), convMap)
//
//    def select(label: String)(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.select(label), convMap)
//
//    def fold: UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.fold, GraphConverterMapper.toList)
//
//    def unfold(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.unfold[Any], convMap)
//
//    def selectKeys(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.select(Column.keys), convMap)
//
//    def selectValues(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.select(Column.values), convMap)
//
//    def limit(max: Long): UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.limit(max), GraphConverterMapper.identity)
//
//    def map[F, T](f: F => T)(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, F](_.map((t: Traverser[F]) => f(t.get)), convMap)
//
//
//    def constant[A](cst: A)(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.constant(cst), convMap)
//
//    def group(k: UntypedBySelector => UntypedByResult, v: UntypedBySelector => UntypedByResult)(
//        implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity
//    ): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](t => v(bySelector)(k(bySelector)(t.group())), convMap)
//
//    def sort(f: (UntypedSortBySelector => UntypedByResult)*): UntypedTraversal =
//      untypedTraversal
//        .onGraphTraversal[Any, Any](t => f.map(_(sortBySelector)).foldLeft(t.order)((acc, s) => s(acc).untyped), GraphConverterMapper.identity)
//
//    def project(
//        f: (UntypedBySelector => UntypedByResult)*
//    )(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](t => f.map(_(bySelector)).foldLeft(t.group.untyped)((acc, p) => p(acc).untyped), convMap)
//
//    def filter(f: UntypedTraversal => UntypedTraversal): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.filter(__.start), GraphConverterMapper.identity)
//
//    def is(predicate: P[_]): UntypedTraversal = untypedTraversal.onGraphTraversal[Any, Any](_.is(predicate), GraphConverterMapper.identity)
//
//    def or(f: (UntypedTraversal => UntypedTraversal)*): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.or(f.map(_(UntypedTraversal.start).traversal): _*), GraphConverterMapper.identity)
//
//    def and(f: (UntypedTraversal => UntypedTraversal)*): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.and(f.map(_(UntypedTraversal.start).traversal): _*), GraphConverterMapper.identity)
//
//    def not(t: UntypedTraversal => UntypedTraversal): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.not(t(UntypedTraversal.start).traversal), GraphConverterMapper.identity)
//
//    def coalesce(
//        f: (UntypedTraversal => UntypedTraversal)*
//    )(implicit convMap: GraphConverterMapper[_, _] = GraphConverterMapper.toIdentity): UntypedTraversal =
//      untypedTraversal.onGraphTraversal[Any, Any](_.coalesce(f.map(_(UntypedTraversal.start).traversal): _*), convMap)
//
//    def property[DD, GG](name: String, mapping: Mapping[_, DD, GG]): Traversal[DD, GG] =
//      untypedTraversal.onGraphTraversal[Any, Any](_.values(name), mapping.toDomain(_)).typed[DD, GG](ClassTag(mapping.graphTypeClass))
//    def _createdBy: Traversal[String, String] = property("_createdBy", UniMapping.string)
//    def _createdAt: Traversal[Date, Date]     = property("_createdAt", UniMapping.date)
//    def _updatedBy: Traversal[String, String] = property("_updatedBy", UniMapping.string)
//    def _updatedAt: Traversal[Date, Date]     = property("_updatedAt", UniMapping.date)
//
//    private def bySelector: UntypedBySelector         = new UntypedBySelector
//    private def sortBySelector: UntypedSortBySelector = new UntypedSortBySelector
//  }

  implicit class TraversalCaster(traversal: Traversal[_, _, _]) {
    def cast[D, G]: Traversal[D, G, Converter[D, G]] = traversal.asInstanceOf[Traversal[D, G, Converter[D, G]]]
  }

  implicit class TraversalOps[D, G, C <: Converter[D, G]](traversal: Traversal[D, G, C]) {
    import gremlin.scala.GremlinScala
    type ConvMapper[DD, GG] = GraphConverterMapper[Converter[D, G], Converter[DD, GG]]

    def raw: GremlinScala[G]          = traversal.raw
    def deepRaw: GraphTraversal[_, G] = traversal.raw.traversal
    def toDomain(g: G): D             = traversal.toDomain(g)
//    def untyped: UntypedTraversal     = new UntypedTraversal(deepRaw, traversal.converter)

    def toIterator: Iterator[D] = {
      val iterator = deepRaw.asScala
      traversal.converter match {
        case _: IdentityConverter[_] => iterator.asInstanceOf[Iterator[D]]
        case _                       => iterator.map(traversal.converter)
      }
    }

    def toSeq: Seq[D] = toIterator.toSeq

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

    def limit(max: Long): Traversal[D, G, C] = traversal.onRaw(_.limit(max))

    def range(low: Long, high: Long): Traversal[D, G, C] = traversal.onRaw(_.range(low, high))

//    def map[A: ClassTag](f: D => A): Traversal[A, G] =
//      new Traversal(raw, new GraphConverter[A, G] {
//        override def apply(g: G): A = traversal.converter.andThen(f)(g)
//      })

    def count: Traversal[Long, JLong, Converter[Long, JLong]] =
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count(), _ => Converter.long)

    def localCount: Traversal[Long, JLong, Converter[Long, JLong]] =
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count(Scope.local), _ => Converter.long)

    def sum: Traversal[D, G, C] =
      traversal.mapAsNumber(_.onDeepRaw(_.sum[Number]))

    def min: Traversal[D, G, C] =
      traversal.mapAsComparable(_.onDeepRaw(_.min[Comparable[G]]))

    def max: Traversal[D, G, C] =
      traversal.mapAsComparable(_.onDeepRaw(_.max[Comparable[G]]))

    def mean: Traversal[D, G, C] =
      traversal.mapAsNumber(_.onDeepRaw(_.mean[Number]))

    def as(stepLabel: StepLabel[D, G, C]): Traversal[D, G, C] = {
      stepLabel.setConverter(traversal.converter)
      traversal.onDeepRaw(_.as(stepLabel.name))
    }

    def constant[A](cst: A): Traversal[A, A, IdentityConverter[A]] =
      traversal.onDeepRawMap[A, A, IdentityConverter[A]](_.constant(cst), _ => Converter.identity[A])

    def group[DK, DV, GK, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]](
        keysBy: GenericBySelector[D, G, C] => ByResult[G, DK, GK, CK],
        valuesBy: GenericBySelector[D, G, C] => ByResult[G, DV, GV, CV]
    ): Traversal[Map[DK, Seq[DV]], JMap[GK, JCollection[GV]], Converter.CMap[DK, DV, GK, GV, CK, CV]] = {
      val keyByResult   = keysBy(genericBySelector)
      val valueByResult = valuesBy(genericBySelector)
      traversal.onDeepRawMap[Map[DK, Seq[DV]], JMap[GK, JCollection[GV]], Converter.CMap[DK, DV, GK, GV, CK, CV]](
        ((_: GraphTraversal[_, G])
          .group
          .asInstanceOf[GraphTraversal[_, G]])
          .andThen(keyByResult)
          .andThen(_.asInstanceOf[GraphTraversal[_, G]])
          .andThen(valueByResult)
          .andThen(_.asInstanceOf[GraphTraversal[Map[DK, Seq[DV]], JMap[GK, JCollection[GV]]]]),
        _ => Converter.cmap[DK, DV, GK, GV, CK, CV](keyByResult.converter, valueByResult.converter)
      )
    }

    def exists: Boolean = traversal.deepRaw.hasNext

    def group[KD, KG, KC <: Converter[KD, KG]](
        keysBy: GenericBySelector[D, G, C] => ByResult[G, KD, KG, KC]
    ): Traversal[Map[KD, Seq[D]], JMap[KG, JCollection[G]], Converter.CMap[KD, D, KG, G, KC, C]] = {
      val keyByResult = keysBy(genericBySelector)
      traversal.onDeepRawMap[Map[KD, Seq[D]], JMap[KG, JCollection[G]], Converter.CMap[KD, D, KG, G, KC, C]](
        ((_: GraphTraversal[_, G])
          .group
          .asInstanceOf[GraphTraversal[_, G]])
          .andThen(keyByResult)
          .andThen(_.asInstanceOf[GraphTraversal[Map[KD, Seq[D]], JMap[KG, JCollection[G]]]]),
        _ => Converter.cmap[KD, D, KG, G, KC, C](keyByResult.converter, traversal.converter)
      )
    }

    def select[DD, GG, CC <: Converter[DD, GG]](label: StepLabel[DD, GG, CC]): Traversal[DD, GG, CC] =
      traversal.onDeepRawMap[DD, GG, CC](_.select(label.name).asInstanceOf[GraphTraversal[DD, GG]], _ => label.converter)

    def fold: Traversal[Seq[D], JList[G], Converter.CList[D, G, C]] =
      traversal.onDeepRawMap[Seq[D], JList[G], Converter.CList[D, G, C]](_.fold(), _ => Converter.clist[D, G, C](traversal.converter))

    def unfold[DU, GU, CC <: Converter[DU, GU]](
        implicit ev: C <:< Poly1Converter[_, _, DU, GU, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.unfold[GU](), _ => traversal.converter.subConverter)

    def sort(f: (SortBySelector[D, G, C] => ByResult[G, G, G, _])*): Traversal[D, G, C] =
      traversal.onDeepRaw(t => f.map(_(sortBySelector)).foldLeft(t.order())((s, g) => g.app(s)))

    /* select only the keys from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectKeys[DU, GU, CC <: Converter[DU, GU]](
        implicit ev: C <:< Poly2Converter[_, _, DU, _, GU, _, CC, _]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.select(Column.keys).asInstanceOf[GraphTraversal[_, GU]], _ => traversal.converter.subConverterKey)

    /* select only the values from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectValues[DU, GU, CC <: Converter[DU, GU]](
        implicit
        ev: C <:< Poly2Converter[_, _, _, DU, _, GU, _, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.select(Column.values).asInstanceOf[GraphTraversal[_, GU]], _ => traversal.converter.subConverterValue)

    def coalesce[DD, GG, CC <: Converter[DD, GG]](f: (Traversal[D, G, C] => Traversal[DD, GG, CC])*): Traversal[DD, GG, CC] = {

      val ts = f.map(_(traversal.start))
      ts.headOption
        .fold(traversal.limit(0).asInstanceOf[Traversal[DD, GG, CC]])(t =>
          traversal.onDeepRawMap[DD, GG, CC](_.coalesce(ts.map(_.deepRaw): _*), _ => t.converter)
        )
    }

    def project[A <: Product: ClassTag](
        builder: ProjectionBuilder[Nil.type, D, G, C] => ProjectionBuilder[A, D, G, C]
    ): Traversal[A, JMap[String, Any], Converter[A, JMap[String, Any]]] = {
      val b: ProjectionBuilder[A, D, G, C] = builder(projectionBuilder)
      traversal.onDeepRawMap[A, JMap[String, Any], Converter[A, JMap[String, Any]]](b.traversal, _ => b.converter)
    }

    def flatProject[PD, PG, PC <: Converter[PD, PG]](
        f: (GenericBySelector[D, G, C] => ByResult[G, PD, PG, PC])*
    ): Traversal[Seq[Any], JMap[String, Any], Converter[Seq[Any], JMap[String, Any]]] = {
      val labels      = f.map(_ => UUID.randomUUID().toString)
      val projections = f.map(_(genericBySelector)).asInstanceOf[Seq[ByResult[JMap[String, Any], PD, PG, PC]]]
      traversal.onDeepRawMap[Seq[Any], JMap[String, Any], Converter[Seq[Any], JMap[String, Any]]](
        t =>
          projections
            .foldLeft(t.project[Any](labels.head, labels.tail: _*).asInstanceOf[GraphTraversal[Any, JMap[String, Any]]])((acc, p) =>
              p(acc).asInstanceOf[GraphTraversal[Any, JMap[String, Any]]]
            )
            .asInstanceOf[GraphTraversal[_, JMap[String, Any]]],
        _ => (m: JMap[String, Any]) => labels.zip(projections).map { case (l, p) => p.converter(m.get(l).asInstanceOf[PG]) }
      )
    }

    def outTo[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] = {
      val model = Model.getEdgeModel[E, _, _]
      model.fromLabel
      model.Traversal(raw.out(model.label))
    }

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

    def getOrFail(entityName: String): Try[D] = orFail(NotFoundError(s"$entityName not found"))
    def orFail(ex: Exception): Try[D]         = headOption().fold[Try[D]](Failure(ex))(Success.apply)

    def has[A](key: String, value: A)(implicit ev: G <:< Element): Traversal[D, G, C]        = traversal.onDeepRaw(_.has(key, P.eq[A](value)))
    def has[A](key: String, predicate: P[A])(implicit ev: G <:< Element): Traversal[D, G, C] = traversal.onDeepRaw(_.has(key, predicate))
    def has[A](key: String)(implicit ev: G <:< Element): Traversal[D, G, C]                  = traversal.onDeepRaw(_.has(key))
    def hasNot[A](key: String, predicate: P[A])(implicit ev: G <:< Element): Traversal[D, G, C] =
      traversal.onDeepRaw(_.not(__.start().has(key, predicate)))
    def hasNot[A](key: String, value: A)(implicit ev: G <:< Element): Traversal[D, G, C] =
      traversal.onDeepRaw(_.not(__.start().has(key, P.eq[A](value))))
    def hasNot[A](key: String): Traversal[D, G, C] = traversal.onDeepRaw(_.has(key))

    def hasId(ids: String*)(implicit ev: G <:< Element): Traversal[D, G, C] =
      ids.headOption.fold(limit(0))(id => traversal.onDeepRaw(_.hasId(id, ids.tail: _*)))

    def _id(implicit ev: G <:< Element) = new Traversal[String, AnyRef, Converter[String, AnyRef]](raw.id(), IdMapping)

    def property[DD, GG](name: String, converter: Converter[DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG, Converter[DD, GG]] =
      new Traversal[DD, GG, Converter[DD, GG]](raw.values[GG](name), converter)
//    def property[DD, GG](name: String, mapping: Mapping[DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG, Converter[DD, GG]] =
//      new Traversal[DD, GG, Converter[DD, GG]](raw.values[GG](name), mapping)
    def _createdBy(implicit ev: G <:< Element): Traversal[String, String, Converter[String, String]] = property("_createdBy", UniMapping.string)
    def _createdAt(implicit ev: G <:< Element): Traversal[Date, Date, Converter[Date, Date]]         = property("_createdAt", UniMapping.date)
    def _updatedBy(implicit ev: G <:< Element): Traversal[String, String, Converter[String, String]] = property("_updatedBy", UniMapping.string)
    def _updatedAt(implicit ev: G <:< Element): Traversal[Date, Date, Converter[Date, Date]]         = property("_updatedAt", UniMapping.date)

    def getByIds(ids: String*): Traversal[D, G, C] =
      traversal.onDeepRaw(t =>
        ids.headOption match {
          case Some(i) => t.hasId(i, ids.tail: _*)
          case None    => t.limit(0)
        }
      )

    def filter(f: Traversal[D, G, C] => Traversal[_, _, _]): Traversal[D, G, C] = traversal.onDeepRaw(_.filter(f(traversal.start).deepRaw))

    def is(predicate: P[G]): Traversal[D, G, C] = traversal.onDeepRaw(_.is(predicate))

    def or(f: (Traversal[D, G, C] => Traversal[D, G, C])*): Traversal[D, G, C] =
      traversal.onDeepRaw(_.or(f.map(_(traversal.start).deepRaw): _*))

    def and(f: (Traversal[D, G, C] => Traversal[D, G, C])*): Traversal[D, G, C] =
      traversal.onDeepRaw(_.and(f.map(_(traversal.start).deepRaw): _*))

    def not(t: Traversal[D, G, C] => Traversal[D, G, C]): Traversal[D, G, C] =
      traversal.onDeepRaw(_.not(t(traversal.start).deepRaw))

    private def projectionBuilder: ProjectionBuilder[Nil.type, D, G, C] =
      new ProjectionBuilder[Nil.type, D, G, C](traversal, Nil, scala.Predef.identity, _ => Nil)
    private def genericBySelector: GenericBySelector[D, G, C] = new GenericBySelector[D, G, C](traversal)
    private def sortBySelector: SortBySelector[D, G, C]       = new SortBySelector[D, G, C](traversal)
  }
}

class ProjectionBuilder[E <: Product, D, G, C <: Converter[D, G]](
    traversal: Traversal[D, G, C],
    labels: Seq[String],
    addBy: GraphTraversal[_, JMap[String, Any]] => GraphTraversal[_, JMap[String, Any]],
    buildResult: JMap[String, Any] => E
) {
  //  def apply[U, TR <: Product](by: By[U])(implicit prepend: TuplePrepend.Aux[E, Tuple1[U], TR]): ProjectionBuilder[TR, T] = {
  //    val label = UUID.randomUUID().toString
  //    new ProjectionBuilder[TR, T](traversal, labels :+ label, addBy.andThen(by.apply), map => buildResult(map) :+ map.get(label).asInstanceOf[U])
  //  }

  def by[TR <: Product](implicit prepend: TuplePrepend.Aux[E, Tuple1[D], TR]): ProjectionBuilder[TR, D, G, C] = {
    val label = UUID.randomUUID().toString
    new ProjectionBuilder[TR, D, G, C](
      traversal,
      labels :+ label,
      addBy.andThen(_.by),
      map => buildResult(map) :+ traversal.converter(map.get(label).asInstanceOf[G])
    )
  }

//  def by[U, TR <: Product](key: Key[U])(implicit prepend: TuplePrepend.Aux[E, Tuple1[U], TR]): ProjectionBuilder[TR, D, G] = {
//    val label = UUID.randomUUID().toString
//    new ProjectionBuilder[TR, D, G](
//      traversal,
//      labels :+ label,
//      addBy.andThen(_.by(key.name)),
//      map => buildResult(map) :+ map.get(label).asInstanceOf[U]
//    )
//  }

  def by[DD, GG, CC <: Converter[DD, GG], TR <: Product](
      f: Traversal[D, G, C] => Traversal[DD, GG, CC]
  )(implicit prepend: TuplePrepend.Aux[E, Tuple1[DD], TR]): ProjectionBuilder[TR, D, G, C] = {
    val label = UUID.randomUUID().toString
    val p     = f(traversal.start)
    new ProjectionBuilder[TR, D, G, C](
      traversal,
      labels :+ label,
      addBy.andThen(_.by(p.raw.traversal)),
      map => buildResult(map) :+ p.converter(map.get(label).asInstanceOf[GG])
    )
  }

  private[steps] def traversal(g: GraphTraversal[_, _]): GraphTraversal[_, JMap[String, Any]] = addBy(g.project(labels.head, labels.tail: _*))
  private[steps] def converter: Converter[E, JMap[String, Any]]                               = buildResult(_)

  //(v1: JMap[String, Any]) => buildResult(v1)
//  private[steps] def build(g: GremlinScala[_]): GremlinScala[E] =
//    GremlinScala(addBy(g.traversal.project(labels.head, labels.tail: _*))).map(buildResult)
//  private[steps] def build(g: GraphTraversal[_, _]): Traversal[E, JMap[String, Any], Converter[E, JMap[String, Any]]] =
//    new Traversal[E, JMap[String, Any], Converter[E, JMap[String, Any]]](
//      addBy(g.project(labels.head, labels.tail: _*)),
//      new Converter[E, JMap[String, Any]] {
//        override def apply(m: JMap[String, Any]): E = buildResult(m)
//      }
//    ) //((m: JMap[String, Any]) => buildResult(m)))
//  //.map[E]((t: Traverser[JMap[String, Any]]) => buildResult(t.get))

}

class GenericBySelector[D, G, C <: Converter[D, G]](origin: Traversal[D, G, C]) {
  def by: ByResult[G, D, G, C] = ByResult[G, D, G, C](origin.converter)(_.by())
  def byLabel[DD, GG, CC <: Converter[DD, GG]](stepLabel: StepLabel[DD, GG, CC]): ByResult[G, DD, GG, CC] =
    ByResult[G, DD, GG, CC](stepLabel.converter)(_.as(stepLabel.name).asInstanceOf[GraphTraversal[_, GG]])
  def by[DD, GG, CC <: Converter[DD, GG]](f: Traversal[D, G, C] => Traversal[DD, GG, CC]): ByResult[G, DD, GG, CC] = {
    val x = f(origin.start)
    ByResult[G, DD, GG, CC](x.converter)(_.by(x.deepRaw).asInstanceOf[GraphTraversal[_, GG]])
  }
}

class SortBySelector[D, G, C <: Converter[D, G]](origin: Traversal[D, G, C]) {
//  def by[B](f: Traversal[D, G] => Traversal[_, B]): ByResult[G, G]               = (_: GraphTraversal[_, G]).by(f(origin.start).deepRaw)
  def by[DD, GG](f: Traversal[D, G, C] => Traversal[DD, GG, _], order: Order): ByResult[G, G, G, IdentityConverter[G]] =
    ByResult[G, G, G, IdentityConverter[G]](Converter.identity)(_.by(f(origin.start).deepRaw, order))
}

abstract class ByResult[F, DD, GG, C <: Converter[DD, GG]](val converter: C) extends (GraphTraversal[_, F] => GraphTraversal[_, GG]) {
  def app[A](t: GraphTraversal[A, F]): GraphTraversal[A, GG] = apply(t).asInstanceOf[GraphTraversal[A, GG]]
}

object ByResult {
  def apply[G, DD, GG, C <: Converter[DD, GG]](converter: C)(f: GraphTraversal[_, G] => GraphTraversal[_, GG]): ByResult[G, DD, GG, C] =
    new ByResult[G, DD, GG, C](converter) {
      override def apply(g: GraphTraversal[_, G]): GraphTraversal[_, GG] = f(g)
    }
}
//class UntypedBySelector[D, G, C <: Converter[D, G]](origin: Traversal[D, G, C]) {
//  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
//
//  def by: ByResult[G, D, G, C]                                     = ByResult[G, D, G, C](origin.converter)(_.by())
//  def by(f: UntypedTraversal => UntypedTraversal): UntypedByResult = (_: GraphTraversal[_, _]).by(f(new UntypedTraversal(__.start())).traversal)
//  def by: ByResult[G, D, G]                                        = ByResult[G, D, G](origin.converter)(_.by())
//  def byLabel[DD, GG](stepLabel: StepLabel[DD, GG]): ByResult[G, DD, GG] =
//    ByResult(stepLabel.converter)(_.as(stepLabel.name).asInstanceOf[GraphTraversal[_, GG]])
//  def by[DD, GG](f: Traversal[D, G] => Traversal[DD, GG]): ByResult[G, DD, GG] = {
//    val x = f(origin.start)
//    ByResult[G, DD, GG](x.converter)(_.by(x.deepRaw).asInstanceOf[GraphTraversal[_, GG]])
//  }
//}
//
//class UntypedSortBySelector {
//  import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
//  def by(f: UntypedTraversal => UntypedTraversal, order: Order): UntypedByResult =
//    (_: GraphTraversal[_, _]).by(f(new UntypedTraversal(__.start())).traversal, order)
//}
//abstract class UntypedByResult[D, G] extends (GraphTraversal[D, G] => GraphTraversal[D, G])

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
//
//    def label(implicit ev: G <:< Element): Traversal[String, String]                  = new Traversal(raw.label(), UniMapping.string)
//    def value[A: ClassTag](name: String)(implicit ev: G <:< Element): Traversal[A, A] = new Traversal[A, A](raw.value[A](name), UniMapping.identity)
//
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
