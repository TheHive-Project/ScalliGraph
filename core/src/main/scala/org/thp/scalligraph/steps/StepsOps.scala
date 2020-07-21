package org.thp.scalligraph.steps

import java.lang.{Long => JLong}
import java.util.{Date, UUID, Collection => JCollection, List => JList, Map => JMap}

import gremlin.scala.{Edge, Element, P, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, GraphTraversal}
import org.apache.tinkerpop.gremlin.process.traversal.{Order, Scope}
import org.apache.tinkerpop.gremlin.structure.Column
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.macros.UpdateMacro
import org.thp.scalligraph.models.{Entity, Model, UniMapping}
import play.api.Logger
import shapeless.ops.tuple.{Prepend => TuplePrepend}
import shapeless.syntax.std.tuple._

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

object StepsOps {
  lazy val logger: Logger = Logger(getClass)

  implicit class TraversalCaster(traversal: Traversal[_, _, _]) {
    def cast[D, G]: Traversal[D, G, Converter[D, G]] = traversal.asInstanceOf[Traversal[D, G, Converter[D, G]]]
  }

  implicit class TraversalOps[D, G, C <: Converter[D, G]](val traversal: Traversal[D, G, C]) {
    import gremlin.scala.GremlinScala

    def raw: GremlinScala[G]          = traversal.raw
    def deepRaw: GraphTraversal[_, G] = traversal.raw.traversal
    def toDomain(g: G): D             = traversal.toDomain(g)

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
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count(), Converter.long)

    def localCount: Traversal[Long, JLong, Converter[Long, JLong]] =
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count(Scope.local), Converter.long)

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
      traversal.onDeepRawMap[A, A, IdentityConverter[A]](_.constant(cst), Converter.identity[A])

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
        Converter.cmap[DK, DV, GK, GV, CK, CV](keyByResult.converter, valueByResult.converter)
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
        Converter.cmap[KD, D, KG, G, KC, C](keyByResult.converter, traversal.converter)
      )
    }

    def select[DD, GG, CC <: Converter[DD, GG]](label: StepLabel[DD, GG, CC]): Traversal[DD, GG, CC] =
      traversal.onDeepRawMap[DD, GG, CC](_.select(label.name).asInstanceOf[GraphTraversal[DD, GG]], label.converter)

    def fold: Traversal[Seq[D], JList[G], Converter.CList[D, G, C]] =
      traversal.onDeepRawMap[Seq[D], JList[G], Converter.CList[D, G, C]](_.fold(), Converter.clist[D, G, C](traversal.converter))

    def unfold[DU, GU, CC <: Converter[DU, GU]](
        implicit ev: C <:< Poly1Converter[_, _, DU, GU, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.unfold[GU](), traversal.converter.subConverter)

    def sort(f: (SortBySelector[D, G, C] => ByResult[G, G, G, _])*): Traversal[D, G, C] =
      traversal.onDeepRaw(t => f.map(_(sortBySelector)).foldLeft(t.order())((s, g) => g.app(s)))

    /* select only the keys from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectKeys[DU, GU, CC <: Converter[DU, GU]](
        implicit ev: C <:< Poly2Converter[_, _, DU, _, GU, _, CC, _]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.select(Column.keys).asInstanceOf[GraphTraversal[_, GU]], traversal.converter.subConverterKey)

    /* select only the values from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectValues[DU, GU, CC <: Converter[DU, GU]](
        implicit
        ev: C <:< Poly2Converter[_, _, _, DU, _, GU, _, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onDeepRawMap[DU, GU, CC](_.select(Column.values).asInstanceOf[GraphTraversal[_, GU]], traversal.converter.subConverterValue)

    def coalesce[DD, GG, CC <: Converter[DD, GG]](f: (Traversal[D, G, C] => Traversal[DD, GG, CC])*): Traversal[DD, GG, CC] = {

      val ts = f.map(_(traversal.start))
      ts.headOption
        .fold(traversal.limit(0).asInstanceOf[Traversal[DD, GG, CC]])(t =>
          traversal.onDeepRawMap[DD, GG, CC](_.coalesce(ts.map(_.deepRaw): _*), t.converter)
        )
    }

    def project[A <: Product: ClassTag](
        builder: ProjectionBuilder[Nil.type, D, G, C] => ProjectionBuilder[A, D, G, C]
    ): Traversal[A, JMap[String, Any], Converter[A, JMap[String, Any]]] = {
      val b: ProjectionBuilder[A, D, G, C] = builder(projectionBuilder)
      traversal.onDeepRawMap[A, JMap[String, Any], Converter[A, JMap[String, Any]]](b.traversal, b.converter)
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
        (m: JMap[String, Any]) => labels.zip(projections).map { case (l, p) => p.converter(m.get(l).asInstanceOf[PG]) }
      )
    }

    def out[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out(ru.typeOf[E].typeSymbol.name.toString), Converter.identity[Vertex])
    def out(label: String)(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out(label), Converter.identity[Vertex])
    def out()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out(), Converter.identity[Vertex])

    def outE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE(ru.typeOf[E].typeSymbol.name.toString), Converter.identity[Edge])
    def outE(label: String)(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE(label), Converter.identity[Edge])
    def outE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE(), Converter.identity[Edge])

    def in[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in(ru.typeOf[E].typeSymbol.name.toString), Converter.identity[Vertex])
    def in(label: String)(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in(label), Converter.identity[Vertex])
    def in()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in(), Converter.identity[Vertex])

    def inE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE(ru.typeOf[E].typeSymbol.name.toString), Converter.identity[Edge])
    def inE(label: String)(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE(label), Converter.identity[Edge])
    def inE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onDeepRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE(), Converter.identity[Edge])

    def inV(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.inV(), Converter.identity[Vertex])
    def outV(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.outV(), Converter.identity[Vertex])
    def otherV(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onDeepRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.otherV(), Converter.identity[Vertex])

    def vertexModel[E <: Product](
        implicit ev: G <:< Vertex,
        model: Model.Vertex[E]
    ): Traversal[E with Entity, Vertex, Converter[E with Entity, Vertex]] =
      traversal.onDeepRawMap[E with Entity, Vertex, Converter[E with Entity, Vertex]](
        _.asInstanceOf[GraphTraversal[_, Vertex]],
        model.converter
      )
    def edgeModel[E <: Product: ru.TypeTag](
        implicit ev: G <:< Edge,
        model: Model.Edge[E, _, _]
    ): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
      traversal.onDeepRawMap[E with Entity, Edge, Converter[E with Entity, Edge]](
        _.asInstanceOf[GraphTraversal[_, Edge]],
        model.converter
      )

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

    def label(implicit ev: G <:< Element) = new Traversal[String, String, IdentityConverter[String]](raw.label(), Converter.identity[String])
    def _id(implicit ev: G <:< Element)   = new Traversal[String, AnyRef, Converter[String, AnyRef]](raw.id(), IdMapping)

    def update(selector: D => Any, value: Any)(implicit ev1: G <:< Element, ev2: D <:< Product with Entity): Traversal[D, G, C] =
      macro UpdateMacro.update
    def updateOne(selector: D => Any, value: Any)(implicit ev1: G <:< Element, ev2: D <:< Product with Entity): Traversal[D, G, C] =
      macro UpdateMacro.update
    def removeProperty(name: String)(implicit ev: G <:< Element): Traversal[D, G, C] = {
      val label = UUID.randomUUID().toString
      traversal.onDeepRaw(_.as(label).properties(name).drop().iterate().select(label))
    }

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
