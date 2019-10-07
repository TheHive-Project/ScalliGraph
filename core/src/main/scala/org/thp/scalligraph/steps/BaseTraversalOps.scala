package org.thp.scalligraph.steps

import java.lang.{Long => JLong}
import java.util.{UUID, Collection => JCollection, Map => JMap}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import gremlin.scala.dsl.Converter
import gremlin.scala.{By, GremlinScala, Key, OrderBy, P, ProjectionBuilder, StepLabel}
import org.apache.tinkerpop.gremlin.structure.Element
import org.thp.scalligraph.AuthorizationError
import org.thp.scalligraph.models.{Mapping, UniMapping}
import shapeless.HNil

trait BaseTraversalOps {

  implicit class BaseTraversalOpsDefs[T <: BaseTraversal](val traversal: T) {

    def raw: GremlinScala[traversal.EndGraph] = traversal.raw

    def converter: Converter.Aux[traversal.EndDomain, traversal.EndGraph] = traversal.converter

    private def newInstance0(newRaw: GremlinScala[traversal.EndGraph]): T = traversal.newInstance(newRaw).asInstanceOf[T]

    def range(from: Long, to: Long): T =
      if (from == 0 && to == Long.MaxValue) traversal
      else newInstance0(traversal.raw.range(from, to))

    def richPage[DD](from: Long, to: Long, withTotal: Boolean)(f: T => TraversalLike[DD, _]): PagedResult[DD] = {
      logger.debug(s"Execution of $raw")
      val size   = if (withTotal) Some(raw.clone().count().head.toLong) else None
      val values = f(traversal.range(from, to)).toList
      PagedResult(values, size)
    }

    def exists(): Boolean = {
      logger.debug(s"Execution of $raw")
      raw.limit(1).traversal.hasNext
    }

    def existsOrFail(): Try[Unit] = if (exists()) Success(()) else Failure(AuthorizationError("Unauthorized action"))

    def getCount: Long = raw.count().head()

    def count: Traversal[Long, JLong] = new Traversal(raw.count(), UniMapping.jlong)

    def sort(orderBys: OrderBy[_]*): T = newInstance0(raw.order(orderBys: _*))

    def filter(f: T => BaseTraversal): T = newInstance0(raw.filter(g => f(newInstance0(g)).raw))

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
      val labels    = bys.map(_ => UUID.randomUUID().toString)
      val traversal = bys.foldLeft(raw.project[A](labels.head, labels.tail: _*))((t, b) => GremlinScala(b.apply(t.traversal)))
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

    def has[A](key: Key[A], predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.has(key, predicate))
    def has[A](key: Key[A])(implicit ev: traversal.EndGraph <:< Element): T                  = newInstance0(raw.has(key))

    def hasNot[A](key: Key[A], predicate: P[A])(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasNot(key, predicate))

    def hasNot[A](key: Key[A]): T = newInstance0(raw.hasNot(key))

    def hasId(ids: String*)(implicit ev: traversal.EndGraph <:< Element): T = newInstance0(raw.hasId(ids: _*))
  }

}
