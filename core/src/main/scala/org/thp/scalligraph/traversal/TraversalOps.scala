package org.thp.scalligraph.traversal

import java.lang.{Long => JLong}
import java.util.{Date, NoSuchElementException, UUID, Collection => JCollection, List => JList, Map => JMap}

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, GraphTraversal}
import org.apache.tinkerpop.gremlin.process.traversal.{P, Scope}
import org.apache.tinkerpop.gremlin.structure._
import org.thp.scalligraph.controllers.Renderer
import org.thp.scalligraph.macros.TraversalMacro
import org.thp.scalligraph.models._
import org.thp.scalligraph.traversal.Converter.CMap
import org.thp.scalligraph.{AuthorizationError, InternalError, NotFoundError}
import play.api.Logger
import shapeless.ops.hlist.{Mapper, RightFolder, ToTraversable, Tupler}
import shapeless.poly._
import shapeless.syntax.std.tuple._
import shapeless.{Generic, HList, HNil}

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

object TraversalOps {
  lazy val logger: Logger = Logger(getClass)

  implicit class TraversalCaster(traversal: Traversal[_, _, _]) {
    def cast[D, G]: Traversal[D, G, Converter[D, G]]                              = traversal.asInstanceOf[Traversal[D, G, Converter[D, G]]]
    def castDomain[D]: Traversal[D, Traversal.UnkG, Converter[D, Traversal.UnkG]] = cast[D, Traversal.UnkG]
  }

  implicit class TraversalOpsDefs[D, G, C <: Converter[D, G]](val traversal: Traversal[D, G, C]) {
    def raw: GraphTraversal[_, G] = traversal.raw
    def toDomain(g: G): D         = traversal.converter(g)

    def toIterator: Iterator[D] = {
      logger.debug(s"Execution of $raw (toIterator)")
      val iterator = raw.asScala
      traversal.converter match {
        case _: IdentityConverter[_] => iterator.asInstanceOf[Iterator[D]]
        case _                       => iterator.map(traversal.converter)
      }
    }

    def toSeq: Seq[D] = {
      logger.debug(s"Execution of $raw (toSeq)")
      toIterator.toVector
    }

    def getCount: Long = {
      logger.debug(s"Execution of $raw (count)")
      raw.count().next()
    }

    def head: D = {
      logger.debug(s"Execution of $raw (head)")
      toDomain(raw.next())
    }

    def headOption: Option[D] = {
      logger.debug(s"Execution of $raw (headOption)")
      if (raw.hasNext) Some(toDomain(raw.next()))
      else None
    }

    def toList: List[D] = {
      logger.debug(s"Execution of $raw (toList)")
      raw.toList.asScala.map(toDomain).toList
    }

    def toSet: Set[D] = {
      logger.debug(s"Execution of $raw (toSet)")
      raw.toList.asScala.map(toDomain).toSet
    }

    def getOrFail(entityName: String): Try[D] = orFail(NotFoundError(s"$entityName not found"))

    def orFail(ex: Exception): Try[D] = headOption.fold[Try[D]](Failure(ex))(Success.apply)

    def exists: Boolean = {
      logger.debug(s"Execution of $raw (exists)")
      traversal.raw.hasNext
    }

    def existsOrFail: Try[Unit] = if (exists) Success(()) else Failure(AuthorizationError("Unauthorized action"))

    def remove(): Unit = {
      logger.debug(s"Execution of $raw (drop)")
      traversal.raw.drop().iterate()
      ()
    }

    def richPage[DD: Renderer, GG, CC <: Converter[DD, GG]](from: Long, to: Long, withTotal: Boolean)(
        f: Traversal[D, G, C] => Traversal[DD, GG, CC]
    ): IteratorOutput = {
      logger.debug(s"Execution of $raw (richPage)")
      val size   = if (withTotal) Some(() => traversal.clone().count.head) else None
      val values = f(traversal.range(from, to))
      IteratorOutput(values, size)
    }

    def limit(max: Long): Traversal[D, G, C] = traversal.onRaw(_.limit(max))

    def range(low: Long, high: Long): Traversal[D, G, C] = traversal.onRaw(_.range(low, high))

//    def map[A: ClassTag](f: D => A): Traversal[A, G] =
//      new Traversal(raw, new GraphConverter[A, G] {
//        override def apply(g: G): A = traversal.converter.andThen(f)(g)
//      })

    def count: Traversal[Long, JLong, Converter[Long, JLong]] =
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count())(Converter.long)

    def localCount: Traversal[Long, JLong, Converter[Long, JLong]] =
      traversal.onRawMap[Long, JLong, Converter[Long, JLong]](_.count(Scope.local))(Converter.long)

    def sum: Traversal[Number, Number, Converter.Identity[Number]] =
      traversal.onRawMap[Number, Number, Converter.Identity[Number]](_.sum[Number])(Converter.identity[Number])

    def min: Traversal[D, G, C] =
      traversal.mapAsComparable(_.onRaw(_.min[Comparable[G]]))

    def max: Traversal[D, G, C] =
      traversal.mapAsComparable(_.onRaw(_.max[Comparable[G]]))

    def mean: Traversal[D, G, C] =
      traversal.mapAsNumber(_.onRaw(_.mean[Number]))

    def as(stepLabel: StepLabel[D, G, C], otherLabels: StepLabel[D, G, C]*): Traversal[D, G, C] = {
      stepLabel.setConverter(traversal.converter)
      traversal.onRaw(_.as(stepLabel.name, otherLabels.map(_.name): _*))
    }

    def constant[A](cst: A): Traversal[A, A, IdentityConverter[A]] =
      traversal.onRawMap[A, A, IdentityConverter[A]](_.constant(cst))(Converter.identity[A])

    def constant2[DD, GG](cst: DD): Traversal[DD, GG, Converter[DD, GG]] =
      traversal.onRawMap[DD, GG, Converter[DD, GG]](_.constant(1.asInstanceOf[GG]))((_: GG) => cst)

    def group[DK, DV, GK, GV, CK <: Converter[DK, GK], CV <: Converter[DV, GV]](
        keysBy: GenericBySelector[D, G, C] => ByResult[G, DK, GK, CK],
        valuesBy: GroupBySelector[D, G, C] => ByResult[G, DV, GV, CV]
    ): Traversal[Map[DK, DV], JMap[GK, GV], Converter.CMap[DK, DV, GK, GV, CK, CV]] = {
      val keyByResult   = keysBy(genericBySelector)
      val valueByResult = valuesBy(groupBySelector)
      traversal.onRawMap[Map[DK, DV], JMap[GK, GV], Converter.CMap[DK, DV, GK, GV, CK, CV]](
        ((_: GraphTraversal[_, G])
          .group
          .asInstanceOf[GraphTraversal[_, G]])
          .andThen(keyByResult)
          .andThen(_.asInstanceOf[GraphTraversal[_, G]])
          .andThen(valueByResult)
          .andThen(_.asInstanceOf[GraphTraversal[Map[DK, DV], JMap[GK, GV]]])
      )(
        Converter.cmap[DK, DV, GK, GV, CK, CV](keyByResult.converter, valueByResult.converter)
      )
    }

    def group[KD, KG, KC <: Converter[KD, KG]](
        keysBy: GenericBySelector[D, G, C] => ByResult[G, KD, KG, KC]
    ): Traversal[Map[KD, Seq[D]], JMap[KG, JCollection[G]], Converter.CGroupMap[KD, D, KG, G, KC, C]] = {
      val keyByResult = keysBy(genericBySelector)
      traversal.onRawMap[Map[KD, Seq[D]], JMap[KG, JCollection[G]], Converter.CGroupMap[KD, D, KG, G, KC, C]](
        ((_: GraphTraversal[_, G])
          .group
          .asInstanceOf[GraphTraversal[_, G]])
          .andThen(keyByResult)
          .andThen(_.asInstanceOf[GraphTraversal[Map[KD, Seq[D]], JMap[KG, JCollection[G]]]])
      )(
        Converter.cgroupMap[KD, D, KG, G, KC, C](keyByResult.converter, traversal.converter)
      )
    }

    def groupCount[KD, KG, KC <: Converter[KD, KG]](
        keysBy: GenericBySelector[D, G, C] => ByResult[G, KD, KG, KC]
    ): Traversal[Map[KD, Long], JMap[KG, JLong], CMap[KD, Long, KG, JLong, KC, Converter[Long, JLong]]] = {
      val keyByResult = keysBy(genericBySelector)
      traversal.onRawMap[Map[KD, Long], JMap[KG, JLong], Converter.CMap[KD, Long, KG, JLong, KC, Converter[Long, JLong]]](t =>
        keyByResult(t.groupCount.asInstanceOf[GraphTraversal[_, G]]).asInstanceOf[GraphTraversal[_, JMap[KG, JLong]]]
      )(
        Converter.cmap[KD, Long, KG, JLong, KC, Converter[Long, JLong]](keyByResult.converter, Converter.long)
      )
    }

    def choose[S, DD](
        branchSelect: BranchSelector[D, G, C] => BranchSelectorOn[D, G, C, S, DD]
    ): Traversal[DD, JMap[String, Any], Converter[DD, JMap[String, Any]]] =
      branchSelect(new BranchSelector[D, G, C](traversal)).build

    def choose[DD, GG, CC <: Converter[DD, GG]](
        predicate: Traversal[D, G, C] => Traversal.Some,
        onTrue: Traversal[D, G, C] => Traversal[DD, GG, CC],
        onFalse: Traversal[D, G, C] => Traversal[DD, GG, CC]
    ): Traversal[DD, GG, CC] = {
      val p = predicate(traversal.start).raw
      val t = onTrue(traversal.start)
      val f = onFalse(traversal.start)
      traversal.onRawMap[DD, GG, CC](_.choose(p, t.raw, f.raw))(t.converter)
    }
    def `match`(
        elements: (MatchElementBuilder.type => MatchElement[_, _, _, _, _, _])*
    ): Traversal[Map[String, Any], JMap[String, Any], CMap[String, Any, String, Any, IdentityConverter[String], IdentityConverter[Any]]] =
      traversal.onRawMap[Map[String, Any], JMap[String, Any], CMap[String, Any, String, Any, IdentityConverter[String], IdentityConverter[Any]]](
        _.`match`(elements.map(_.apply(MatchElementBuilder).traversal): _*)
      )(
        Converter.cmap[String, Any, String, Any, IdentityConverter[String], IdentityConverter[Any]](
          Converter.identity[String],
          Converter.identity[Any]
        )
      )

    def select[DD, GG, CC <: Converter[DD, GG]](label: StepLabel[DD, GG, CC]): Traversal[DD, GG, CC] =
      traversal.onRawMap[DD, GG, CC](_.select(label.name).asInstanceOf[GraphTraversal[DD, GG]])(label.converter)

    def select[LT <: Product, LL <: HList, CL <: HList, CT, LN <: HList](
        labels: LT
    )(implicit
        gen: Generic.Aux[LT, LL],
        folder: RightFolder.Aux[LL, (JMap[String, Any], HNil), SelectLabelConverter.type, (JMap[String, Any], CL)],
        tupler: Tupler.Aux[CL, CT],
        mapper: Mapper.Aux[SelectLabelName.type, LL, LN],
        toTraversal: ToTraversable.Aux[LN, List, String]
    ): Traversal[CT, JMap[String, Any], Converter[CT, JMap[String, Any]]] = {
      val labelHlist: LL = gen.to(labels)
      val converter = new Converter[CT, JMap[String, Any]] {
        override def apply(jmap: JMap[String, Any]): CT =
          tupler(labelHlist.foldRight(jmap -> (HNil: HNil))(SelectLabelConverter)._2)
      }
      labelHlist.map(SelectLabelName).toList(toTraversal) match {
        case first :: second :: other =>
          traversal.onRawMap[CT, JMap[String, Any], Converter[CT, JMap[String, Any]]](_.select(first, second, other: _*))(converter)
        case _ =>
          throw InternalError("select can't be used with only one label")
      }
    }

    def select[R](f: SelectBySelector[Unit] => SelectBySelector[R]): Traversal[R, JMap[String, Any], Converter[R, JMap[String, Any]]] = {
      val selector = f(new SelectBySelector[Unit](Nil, identity, _ => ()))
      selector.labels match {
        case first :: second :: other =>
          traversal.onRawMap[R, JMap[String, Any], Converter[R, JMap[String, Any]]](t => selector.addBys(t.select(first, second, other: _*)))(
            (g: JMap[String, Any]) => selector.converter(g)
          )
        case _ =>
          throw InternalError("select can't be used with only one label")
      }
    }

    def fold: Traversal[Seq[D], JList[G], Converter.CList[D, G, C]] =
      traversal.onRawMap[Seq[D], JList[G], Converter.CList[D, G, C]](_.fold())(Converter.clist[D, G, C](traversal.converter))

    def unfold[DU, GU, CC <: Converter[DU, GU]](implicit
        ev: C <:< Poly1Converter[_, _, DU, GU, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onRawMap[DU, GU, CC](_.unfold[GU]())(traversal.converter.subConverter)

    def sort(f: (SortBySelector[D, G, C] => ByResult[G, G, G, IdentityConverter[G]])*): Traversal[D, G, C] =
      traversal.onRaw(t => f.map(_(sortBySelector)).foldLeft(t.order())((s, g) => g.app(s)))

    /* select only the keys from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectKeys[DU, GU, CC <: Converter[DU, GU]](implicit
        ev: C <:< Poly2Converter[_, _, DU, _, GU, _, CC, _]
    ): Traversal[DU, GU, CC] =
      traversal.onRawMap[DU, GU, CC](_.select(Column.keys).asInstanceOf[GraphTraversal[_, GU]])(traversal.converter.subConverterKey)

    /* select only the values from a map (e.g. groupBy) - see usage examples in SelectSpec.scala */
    def selectValues[DU, GU, CC <: Converter[DU, GU]](implicit
        ev: C <:< Poly2Converter[_, _, _, DU, _, GU, _, CC]
    ): Traversal[DU, GU, CC] =
      traversal.onRawMap[DU, GU, CC](_.select(Column.values).asInstanceOf[GraphTraversal[_, GU]])(traversal.converter.subConverterValue)

    def coalesce[DD, GG, CC <: Converter[DD, GG]](f: (Traversal[D, G, C] => Traversal[DD, GG, CC])*): Traversal[DD, GG, CC] = {

      val ts = f.map(_(traversal.start))
      ts.headOption
        .fold(traversal.limit(0).asInstanceOf[Traversal[DD, GG, CC]])(t => traversal.onRawMap[DD, GG, CC](_.coalesce(ts.map(_.raw): _*))(t.converter))
    }

    def project[A <: Product](
        builder: ProjectionBuilder[Nil.type, D, G, C] => ProjectionBuilder[A, D, G, C]
    ): Traversal[A, JMap[String, Any], Converter[A, JMap[String, Any]]] = {
      val b: ProjectionBuilder[A, D, G, C] = builder(projectionBuilder)
      traversal.onRawMap[A, JMap[String, Any], Converter[A, JMap[String, Any]]](b.traversal)(b.converter)
    }

    def flatProject[PD, PG, PC <: Converter[PD, PG]](
        f: (GenericBySelector[D, G, C] => ByResult[G, PD, PG, PC])*
    ): Traversal[Seq[Any], JMap[String, Any], Converter[Seq[Any], JMap[String, Any]]] = {
      val labels      = f.map(_ => UUID.randomUUID().toString)
      val projections = f.map(_(genericBySelector)).asInstanceOf[Seq[ByResult[JMap[String, Any], PD, PG, PC]]]
      traversal.onRawMap[Seq[Any], JMap[String, Any], Converter[Seq[Any], JMap[String, Any]]](t =>
        projections
          .foldLeft(t.project[Any](labels.head, labels.tail: _*).asInstanceOf[GraphTraversal[Any, JMap[String, Any]]])((acc, p) =>
            p(acc).asInstanceOf[GraphTraversal[Any, JMap[String, Any]]]
          )
          .asInstanceOf[GraphTraversal[_, JMap[String, Any]]]
      )((m: JMap[String, Any]) => labels.zip(projections).map { case (l, p) => p.converter(m.get(l).asInstanceOf[PG]) })
    }

    def out[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out(ru.typeOf[E].typeSymbol.name.toString))(Converter.identity[Vertex])
    def out(label: String)(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out(label))(Converter.identity[Vertex])
    def out()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.out())(Converter.identity[Vertex])

    def outE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE(ru.typeOf[E].typeSymbol.name.toString))(Converter.identity[Edge])
    def outE(label: String)(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE(label))(Converter.identity[Edge])
    def outE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.outE())(Converter.identity[Edge])

    def in[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in(ru.typeOf[E].typeSymbol.name.toString))(Converter.identity[Vertex])
    def in(label: String)(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in(label))(Converter.identity[Vertex])
    def in()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.in())(Converter.identity[Vertex])

    def inE[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE(ru.typeOf[E].typeSymbol.name.toString))(Converter.identity[Edge])
    def inE(label: String)(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE(label))(Converter.identity[Edge])
    def inE()(implicit ev: G <:< Vertex): Traversal[Edge, Edge, IdentityConverter[Edge]] =
      traversal.onRawMap[Edge, Edge, IdentityConverter[Edge]](_.inE())(Converter.identity[Edge])

    def inV(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.inV())(Converter.identity[Vertex])
    def outV(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.outV())(Converter.identity[Vertex])
    def otherV()(implicit ev: G <:< Edge): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.otherV())(Converter.identity[Vertex])

    def both[E <: Product: ru.TypeTag](implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.both(ru.typeOf[E].typeSymbol.name.toString))(Converter.identity[Vertex])
    def both(label: String)(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.both(label))(Converter.identity[Vertex])
    def both()(implicit ev: G <:< Vertex): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.both())(Converter.identity[Vertex])

    def v[E <: Product](implicit
        ev: G <:< Vertex,
        model: Model.Vertex[E]
    ): Traversal[E with Entity, Vertex, Converter[E with Entity, Vertex]] =
      traversal.onRawMap[E with Entity, Vertex, Converter[E with Entity, Vertex]](_.asInstanceOf[GraphTraversal[_, Vertex]])(
        model.converter
      )

    def e[E <: Product](implicit
        ev: G <:< Edge,
        model: Model.Edge[E]
    ): Traversal.E[E] =
      traversal.onRawMap[E with Entity, Edge, Converter[E with Entity, Edge]](_.asInstanceOf[GraphTraversal[_, Edge]])(
        model.converter
      )

    def V[E](ids: String*): Traversal[Vertex, Vertex, IdentityConverter[Vertex]] =
      traversal.onRawMap[Vertex, Vertex, IdentityConverter[Vertex]](_.V(ids: _*))(Converter.identity)

    def entity(implicit ev: G <:< Element): Traversal[Product with Entity, Element, Converter[Product with Entity, Element]] =
      traversal.onRawMap[Product with Entity, Element, Converter[Product with Entity, Element]](_.asInstanceOf[GraphTraversal[_, Element]]) {
        (element: Element) =>
          new Product with Entity {
            override def productElement(n: Int): Any = throw new NoSuchElementException(s"Product0.productElement($n)")

            override def productArity: Int = 0

            override val _id: String                = element.id().toString
            override val _label: String             = element.label()
            override val _createdBy: String         = UMapping.string.getProperty(element, "_createdBy")
            override val _updatedBy: Option[String] = UMapping.string.optional.getProperty(element, "_updatedBy")
            override val _createdAt: Date           = UMapping.date.getProperty(element, "_createdAt")
            override val _updatedAt: Option[Date]   = UMapping.date.optional.getProperty(element, "_createdAt")

            override def canEqual(that: Any): Boolean =
              that match {
                case entity: Entity => entity._id == _id
                case _              => false
              }
          }
      }

    def hasLabel(name: String): Traversal[D, G, C] = traversal.onRaw(_.hasLabel(name))

    def has(accessor: T, predicate: P[_])(implicit ev: G <:< Element): Traversal[D, G, C]    = traversal.onRaw(_.has(accessor, predicate))
    def has[A](key: String, value: A)(implicit ev: G <:< Element): Traversal[D, G, C]        = traversal.onRaw(_.has(key, P.eq[A](value)))
    def has[A](key: String, predicate: P[A])(implicit ev: G <:< Element): Traversal[D, G, C] = traversal.onRaw(_.has(key, predicate))
    def has[A](key: String)(implicit ev: G <:< Element): Traversal[D, G, C]                  = traversal.onRaw(_.has(key))
    def hasNot[A](key: String, predicate: P[A])(implicit ev: G <:< Element): Traversal[D, G, C] =
      traversal.onRaw(_.not(__.start().has(key, predicate)))
    def hasNot[A](key: String, value: A)(implicit ev: G <:< Element): Traversal[D, G, C] =
      traversal.onRaw(_.not(__.start().has(key, P.eq[A](value))))
    def hasNot[A](key: String): Traversal[D, G, C] = traversal.onRaw(_.hasNot(key))

    def hasId(ids: String*)(implicit ev: G <:< Element): Traversal[D, G, C] =
      ids.headOption.fold(limit(0))(id => traversal.onRaw(_.hasId(id, ids.tail: _*)))

    def where(predicate: P[String]): Traversal[D, G, C] = traversal.onRaw(_.where(predicate))
    def where[DD, GG, CC <: Converter[DD, GG]](f: Traversal[D, G, C] => Traversal[DD, GG, CC]): Traversal[D, G, C] =
      traversal.onRaw(_.where(f(traversal.start).raw))

    def label(implicit ev: G <:< Element) = new Traversal[String, String, IdentityConverter[String]](raw.label(), Converter.identity[String])
    def _id(implicit ev: G <:< Element)   = new Traversal[String, AnyRef, Converter[String, AnyRef]](raw.id(), IdMapping)

    def update[V](selector: D => V, value: V)(implicit ev1: G <:< Element, ev2: D <:< Product with Entity): Traversal[D, G, C] =
      macro TraversalMacro.update[V]

    def sideEffect[DD, GG, CC <: Converter[DD, GG]](effect: Traversal[D, G, C] => Traversal[DD, GG, CC]): Traversal[D, G, C] =
      traversal.onRaw(_.sideEffect(effect(traversal.start).raw))

    def value[DD](
        selector: D => DD
    )(implicit
        mapping: UMapping[DD],
        ev1: G <:< Element,
        ev2: D <:< Product with Entity
    ): Traversal[mapping.SingleType, mapping.GraphType, Converter[mapping.SingleType, mapping.GraphType]] = macro TraversalMacro.getValue[DD]

    def removeProperty(name: String)(implicit ev: G <:< Element): Traversal[D, G, C] =
      traversal.sideEffect(
        _.onRaw(t => t.properties[Any](name).drop().asInstanceOf[t.type])
      )

    def property[DD, GG](name: String, converter: Converter[DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG, Converter[DD, GG]] =
      new Traversal[DD, GG, Converter[DD, GG]](raw.values[GG](name), converter)
//    def property[DD, GG](name: String, mapping: Mapping[DD, GG])(implicit ev: G <:< Element): Traversal[DD, GG, Converter[DD, GG]] =
//      new Traversal[DD, GG, Converter[DD, GG]](raw.values[GG](name), mapping)
    def _createdBy(implicit ev: G <:< Element): Traversal[String, String, Converter[String, String]] = property("_createdBy", UMapping.string)
    def _createdAt(implicit ev: G <:< Element): Traversal[Date, Date, Converter[Date, Date]]         = property("_createdAt", UMapping.date)
    def _updatedBy(implicit ev: G <:< Element): Traversal[String, String, Converter[String, String]] = property("_updatedBy", UMapping.string)
    def _updatedAt(implicit ev: G <:< Element): Traversal[Date, Date, Converter[Date, Date]]         = property("_updatedAt", UMapping.date)

    def getByIds(ids: String*): Traversal[D, G, C] =
      traversal.onRaw(t =>
        ids.headOption match {
          case Some(i) => t.hasId(i, ids.tail: _*)
          case None    => t.limit(0)
        }
      )

    def page(from: Long, to: Long, withTotal: Boolean)(implicit renderer: Renderer[D]): IteratorOutput = richPage(from, to, withTotal)(identity)

    def filter(f: Traversal[D, G, C] => Traversal[_, _, _]): Traversal[D, G, C] = traversal.onRaw(_.filter(f(traversal.start).raw))
    def filterNot(f: Traversal[D, G, C] => Traversal[_, _, _]): Traversal[D, G, C] =
      traversal.onRaw(_.filter(f(traversal.start).raw.count().is(P.eq(0))))

    def dedup: Traversal[D, G, C]                              = traversal.onRaw(_.dedup())
    def dedup(labels: StepLabel[_, _, _]*): Traversal[D, G, C] = traversal.onRaw(_.dedup(labels.map(_.name): _*))

    def aggregate(label: StepLabel[D, G, C]): Traversal[D, G, C] = traversal.onRaw(_.aggregate(label.name))

    def flatMap[DD, GG, CC <: Converter[DD, GG]](f: Traversal[D, G, C] => Traversal[DD, GG, CC]): Traversal[DD, GG, CC] = {
      val t = f(traversal.start)
      traversal.onRawMap[DD, GG, CC](_.flatMap(t.raw))(t.converter)
    }

    def unionFlat[DD, GG, CC <: Converter[DD, GG]](traversals: (Traversal[D, G, C] => Traversal[DD, GG, CC])*): Traversal[DD, GG, CC] = {
      val traversalResults = traversals.map(_.apply(traversal.start))
      traversalResults.headOption.fold(traversal.limit(0).asInstanceOf[Traversal[DD, GG, CC]]) { firstTraversal =>
        traversal.onRawMap[DD, GG, CC](_.union(traversalResults.map(_.raw): _*))(firstTraversal.converter)
      }
    }

    def sack[R](f: (R, G) => R): Traversal[D, G, C] =
      traversal.onRaw(_.sack((s: R, g: G) => f(s, g)))

    def sack[R, DD, GG](f: (R, GG) => R, by: GenericBySelector[D, G, C] => ByResult[G, DD, GG, Converter[DD, GG]]): Traversal[D, G, C] = {
      val byResult = by(genericBySelector)
      traversal.onRaw(t => byResult(t.sack((s: R, t: GG) => f(s, t))).asInstanceOf[t.type])
    }

    def sack[R]: Traversal[R, R, Converter.Identity[R]] = traversal.onRawMap[R, R, Converter.Identity[R]](_.sack[R]())(Converter.identity)

    def is(predicate: P[G]): Traversal[D, G, C] = traversal.onRaw(_.is(predicate))

    def or(f: (Traversal[D, G, C] => Traversal[_, _, _])*): Traversal[D, G, C] =
      traversal.onRaw(_.or(f.map(_(traversal.start).raw): _*))

    def and(f: (Traversal[D, G, C] => Traversal[D, G, C])*): Traversal[D, G, C] =
      traversal.onRaw(_.and(f.map(_(traversal.start).raw): _*))

    def not(t: Traversal[D, G, C] => Traversal[D, G, C]): Traversal[D, G, C] =
      traversal.onRaw(_.not(t(traversal.start).raw))

    private def projectionBuilder: ProjectionBuilder[Nil.type, D, G, C] =
      new ProjectionBuilder[Nil.type, D, G, C](traversal, Nil, scala.Predef.identity, _ => Nil)
    private def genericBySelector: GenericBySelector[D, G, C] = new GenericBySelector[D, G, C](traversal)
    private def groupBySelector: GroupBySelector[D, G, C]     = new GroupBySelector[D, G, C](traversal)
    private def sortBySelector: SortBySelector[D, G, C]       = new SortBySelector[D, G, C](traversal)
  }
}
