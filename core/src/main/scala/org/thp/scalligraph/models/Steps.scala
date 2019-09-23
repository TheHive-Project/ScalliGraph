package org.thp.scalligraph.models

import java.util.{Date, UUID, Collection => JCollection, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

import play.api.Logger
import play.api.libs.json.JsObject

import gremlin.scala._
import gremlin.scala.dsl._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.query.PropertyUpdater
import org.thp.scalligraph.services.RichElement
import org.thp.scalligraph.{AuthorizationError, InternalError, NotFoundError}

case class PagedResult[R](result: Seq[R], totalSize: Option[Long]) {
  def map[T](f: R => T): PagedResult[T] = PagedResult(result.map(f), totalSize)
}

abstract class ScalliSteps[EndDomain, EndGraph, ThisStep <: ScalliSteps[EndDomain, EndGraph, ThisStep]] { _: ThisStep =>
  def typeName: String
  override def clone(): ThisStep = newInstance(raw.clone())
  def newInstance(raw: GremlinScala[EndGraph]): ThisStep
  val converter: Converter.Aux[EndDomain, EndGraph]
  val raw: GremlinScala[EndGraph]

  lazy val logger: Logger = Logger(getClass)

  def toList: List[EndDomain] = {
    logger.debug(s"Execution of $raw")
    raw.toList.map(converter.toDomain)
  }

  def toIterator: Iterator[EndDomain] = {
    logger.debug(s"Execution of $raw")
    raw.traversal.asScala.map(converter.toDomain)
  }

  def range(from: Long, to: Long): ThisStep =
    if (from == 0 && to == Long.MaxValue) newInstance(raw) //this
    else newInstance(raw.range(from, to))

  def page(from: Long, to: Long, withTotal: Boolean): PagedResult[EndDomain] = {
    logger.debug(s"Execution of $raw")
    val size   = if (withTotal) Some(raw.clone().count().head.toLong) else None
    val values = range(from, to).asInstanceOf[ScalliSteps[EndDomain, EndGraph, _]].toList
    PagedResult(values, size)
  }

  def richPage[NewEndDomain](from: Long, to: Long, withTotal: Boolean)(
      f: ThisStep => GremlinScala[NewEndDomain]
  ): PagedResult[NewEndDomain] = {
    logger.debug(s"Execution of $raw")
    val size   = if (withTotal) Some(raw.clone().count().head.toLong) else None
    val values = f(range(from, to)).toList
    PagedResult(values, size)
  }

  def head(): EndDomain = {
    logger.debug(s"Execution of $raw")
    converter.toDomain(raw.head)
  }

  def headOption(): Option[EndDomain] = {
    logger.debug(s"Execution of $raw")
    raw.headOption().map(converter.toDomain)
  }

  def exists(): Boolean = {
    logger.debug(s"Execution of $raw")
    raw.limit(1).traversal.hasNext
  }

  def existsOrFail(): Try[Unit] = if (exists()) Success(()) else Failure(AuthorizationError("Unauthorized action"))

  def getOrFail(): Try[EndDomain] =
    headOption()
      .fold[Try[EndDomain]](Failure(NotFoundError(s"$typeName not found")))(Success.apply)

  def orFail(ex: Exception): Try[EndDomain] = headOption().fold[Try[EndDomain]](Failure(ex))(Success.apply)

  def count: Long = raw.count().head()

  def sort(orderBys: OrderBy[_]*): ThisStep = newInstance(raw.order(orderBys: _*))

  def filter(f: GremlinScala[EndGraph] => GremlinScala[_]): ThisStep = newInstance(raw.filter(f))

  def where(f: ThisStep => ScalliSteps[_, _, _]): ThisStep = newInstance(raw.filter(g => f(newInstance(g)).raw))

  def or(f: (ThisStep => ScalliSteps[_, _, _])*): ThisStep = {
    val filters = f.map(r => (g: GremlinScala[EndGraph]) => r(newInstance(g)).raw)
    newInstance(raw.or(filters: _*))
  }

  def and(f: (ThisStep => ScalliSteps[_, _, _])*): ThisStep = {
    val filters = f.map(r => (g: GremlinScala[EndGraph]) => r(newInstance(g)).raw)
    newInstance(raw.and(filters: _*))
  }

  def has[A](key: Key[A], predicate: P[A])(implicit ev: EndGraph <:< Element): ThisStep = newInstance(raw.has(key, predicate))

  def hasNot[A](key: Key[A], predicate: P[A])(implicit ev: EndGraph <:< Element): ThisStep = newInstance(raw.hasNot(key, predicate))

  def hasNot[A](key: Key[A])(implicit ev: EndGraph <:< Element): ThisStep = newInstance(raw.hasNot(key))

  def map[NewEndDomain: ClassTag](f: EndDomain => NewEndDomain): ScalarSteps[NewEndDomain] =
    new ScalarSteps[NewEndDomain](raw.map(x => f(converter.toDomain(x))))

  def groupBy[K, V](k: By[K], v: By[V]): ScalarSteps[JMap[K, JCollection[V]]] =
    new ScalarSteps(raw.group(k, v))

  def groupBy[K](k: GremlinScala[K]): ScalarSteps[JMap[K, JCollection[EndGraph]]] = new ScalarSteps(raw.group(By(k)))

  def fold: ScalarSteps[JList[EndGraph]] = new ScalarSteps(raw.fold)

  def unfold[A: ClassTag] = new ScalarSteps(raw.unfold[A]())

  def project[T <: Product: ClassTag](builder: ProjectionBuilder[Nil.type] => ProjectionBuilder[T]) = new ScalarSteps(raw.project(builder))

  def project[A](bys: By[_ <: A]*): ScalarSteps[JCollection[A]] = {
    val labels    = bys.map(_ => UUID.randomUUID().toString)
    val traversal = bys.foldLeft(raw.project[A](labels.head, labels.tail: _*))((t, b) => GremlinScala(b.apply(t.traversal)))
    ScalarSteps(traversal.selectValues)
  }

  def as[A](stepLabel: StepLabel[A]): ThisStep = newInstance(new GremlinScala[EndGraph](raw.traversal.as(stepLabel.name)))

  def order(orderBys: List[OrderBy[_]]): ThisStep = newInstance(raw.order(orderBys: _*))

  def iterate(): ThisStep = newInstance(raw.iterate())

  def onlyOneOf[A](elements: JList[A]): A = {
    val size = elements.size
    if (size == 1) elements.get(0)
    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
    else throw InternalError(s"No element found")
  }

  def atMostOneOf[A](elements: JList[A]): Option[A] = {
    val size = elements.size
    if (size == 1) Some(elements.get(0))
    else if (size > 1) throw InternalError(s"Too many elements in result ($size found)")
    else None
  }

  def remove(): Unit = {
    raw.drop().iterate()
    ()
  }
//  def union(u: (ThisStep => ScalliSteps[_, _, _])*) = {
//    val unionTraversals = u.map(st => (t: GremlinScala.Aux[EndGraph, HNil]) => st(newInstance(t)).raw)
//    raw.unionFlat(unionTraversals: _*)
//  }
}

class ValueMap(m: Map[String, Seq[AnyRef]]) {
  def get[A](name: String): A = m(name).head.asInstanceOf[A]
}

object ValueMap {

  def unapply(m: JMap[_, _]): Option[ValueMap] =
    Some(new ValueMap(m.asScala.map { case (k, v) => k.toString -> v.asInstanceOf[JList[AnyRef]].asScala.toSeq }.toMap))
}

final class ScalarSteps[T: ClassTag](val raw: GremlinScala[T]) extends ScalliSteps[T, T, ScalarSteps[T]] {

  override val converter: Converter.Aux[T, T] = Converter.identityConverter[T]
  lazy val typeName: String                   = implicitly[ClassTag[T]].runtimeClass.getSimpleName

  override def newInstance(raw: GremlinScala[T]): ScalarSteps[T] = new ScalarSteps[T](raw)
}

object ScalarSteps {

  def apply[T: ClassTag](raw: GremlinScala[T]): ScalarSteps[T] =
    new ScalarSteps[T](raw)
}

abstract class ElementSteps[E <: Product: ru.TypeTag, EndGraph <: Element, ThisStep <: ElementSteps[E, EndGraph, ThisStep]](
    val raw: GremlinScala[EndGraph]
)(implicit val db: Database, graph: Graph)
    extends ScalliSteps[E with Entity, EndGraph, ThisStep] { _: ThisStep =>
  override val converter: Converter.Aux[E with Entity, EndGraph] =
    db.getModel[E].converter(db, graph).asInstanceOf[Converter.Aux[E with Entity, EndGraph]]

  lazy val typeName: String = ru.typeOf[E].toString

  def getByIds(ids: String*): ThisStep = newInstance(raw.hasId(ids: _*))

  def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[E with Entity] =
    db.update(raw, fields, db.getModel[E], graph, authContext)

  def onlyOneOfEntity[A <: Product: ru.TypeTag](elements: JList[_ <: Element]): A with Entity = {
    val size = elements.size
    if (size == 1) elements.get(0).as[A]
    else if (size > 1) throw InternalError(s"Too many ${ru.typeOf[A]} in result ($size found)")
    else throw InternalError(s"No ${ru.typeOf[A]} found")
  }
}

abstract class BaseVertexSteps[E <: Product: ru.TypeTag, ThisStep <: BaseVertexSteps[E, ThisStep]](raw: GremlinScala[Vertex])(
    implicit db: Database,
    val graph: Graph
) extends ElementSteps[E, Vertex, ThisStep](raw) { _: ThisStep =>

  def get(vertex: Vertex): ThisStep = newInstance(raw.V(vertex))

  private[scalligraph] def updateProperties(propertyUpdaters: Seq[PropertyUpdater])(implicit authContext: AuthContext): Try[(ThisStep, JsObject)] = {
    val myClone = clone()
    raw.headOption().fold[Try[(ThisStep, JsObject)]](Failure(NotFoundError(s"$typeName not found"))) { vertex =>
      logger.trace(s"Update ${vertex.id()} by ${authContext.userId}")
      propertyUpdaters
        .toTry(u => u(vertex, db, graph, authContext))
        .map { o =>
          db.setOptionProperty(vertex, "_updatedAt", Some(new Date), db.updatedAtMapping)
          db.setOptionProperty(vertex, "_updatedBy", Some(authContext.userId), db.updatedByMapping)
          myClone -> o.reduceOption(_ ++ _).getOrElse(JsObject.empty)
        }
    }
  }
}

final class VertexSteps[E <: Product: ru.TypeTag](raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph)
    extends BaseVertexSteps[E, VertexSteps[E]](raw) {
  override def newInstance(raw: GremlinScala[Vertex]): VertexSteps[E] = new VertexSteps[E](raw)
}

abstract class BaseEdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product, ThisStep <: BaseEdgeSteps[E, FROM, TO, ThisStep]](
    raw: GremlinScala[Edge]
)(implicit db: Database, val graph: Graph)
    extends ElementSteps[E, Edge, ThisStep](raw) { _: ThisStep =>
}

final class EdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product](raw: GremlinScala[Edge])(implicit db: Database, graph: Graph)
    extends BaseEdgeSteps[E, FROM, TO, EdgeSteps[E, FROM, TO]](raw) {
  override def newInstance(raw: GremlinScala[Edge]): EdgeSteps[E, FROM, TO] = new EdgeSteps[E, FROM, TO](raw)
}
