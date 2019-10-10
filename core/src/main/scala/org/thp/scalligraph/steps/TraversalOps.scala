package org.thp.scalligraph.steps

import java.lang.{Double => JDouble}
import java.util.{Collection => JCollection, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

import gremlin.scala.dsl.Converter
import gremlin.scala.{By, Edge, Element, GremlinScala, Key, P, Vertex}
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.models.UniMapping

trait TraversalOps extends BaseTraversalOps {

  implicit class TraversalOpsDefs[D, G](val traversal: TraversalLike[D, G]) {
    def raw: GremlinScala[G] = traversal.raw

    def converter: Converter.Aux[D, G] = traversal.converter

    private def newInstance0(newRaw: GremlinScala[G]): Traversal[D, G] = traversal.newInstance(newRaw).asInstanceOf[Traversal[D, G]]

    def toList: List[D] = {
      logger.debug(s"Execution of $raw")
      raw.toList.map(converter.toDomain)
    }

    def toIterator: Iterator[D] = {
      logger.debug(s"Execution of $raw")
      raw.traversal.asScala.map(converter.toDomain)
    }

    def page(from: Long, to: Long, withTotal: Boolean): PagedResult[D] = {
      logger.debug(s"Execution of $raw")
      val size            = if (withTotal) Some(raw.clone().count().head.toLong) else None
      val r               = traversal.range(from, to)
      val values: List[D] = r.toList
      PagedResult(values, size)
    }

    def head(): D = {
      logger.debug(s"Execution of $raw")
      converter.toDomain(raw.head)
    }

    def headOption(): Option[D] = {
      logger.debug(s"Execution of $raw")
      raw.headOption().map(converter.toDomain)
    }

    def getOrFail(): Try[D] =
      headOption()
        .fold[Try[D]](Failure(NotFoundError(s"${traversal.typeName} not found")))(Success.apply)

    def orFail(ex: Exception): Try[D] = headOption().fold[Try[D]](Failure(ex))(Success.apply)

    def or(f: (Traversal[D, G] => BaseTraversal)*): Traversal[D, G] = {
      val filters = f.map(r => (g: GremlinScala[G]) => r(newInstance0(g)).raw)
      newInstance0(raw.or(filters: _*))
    }

    def and(f: (Traversal[D, G] => BaseTraversal)*): Traversal[D, G] = {
      val filters = f.map(r => (g: GremlinScala[G]) => r(newInstance0(g)).raw)
      newInstance0(raw.and(filters: _*))
    }

    def sum[N <: Number: ClassTag]()(implicit toNumber: G => N): Traversal[N, N] =
      new Traversal(raw.sum[N]()(toNumber), UniMapping.identity)

    def min[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      new Traversal(raw.min[C]()(toComparable), UniMapping.identity)

    def max[C <: Comparable[_]: ClassTag]()(implicit toComparable: G => C): Traversal[C, C] =
      new Traversal(raw.max[C]()(toComparable), UniMapping.identity)

    def mean[N <: Number]()(implicit toNumber: G => N): Traversal[Double, JDouble] = new Traversal(raw.mean, UniMapping.jdouble)

    def is(predicate: P[G]): Traversal[D, G] = newInstance0(raw.is(predicate))

    def map[A: ClassTag](f: D => A): Traversal[A, A] =
      new Traversal[A, A](raw.map(x => f(converter.toDomain(x))), UniMapping.identity)

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

    def label: Traversal[String, String]                                             = new Traversal(raw.label(), UniMapping.string)
    def value[A: ClassTag](key: Key[A])(implicit ev: G <:< Element): Traversal[A, A] = new Traversal[A, A](raw.value(key), UniMapping.identity)
  }
}
