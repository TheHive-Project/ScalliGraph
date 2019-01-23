package org.thp.scalligraph.models

import java.util.{List ⇒ JList, Map ⇒ JMap}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe ⇒ ru}
import play.api.Logger
import gremlin.scala._
import gremlin.scala.dsl._
import org.thp.scalligraph.{InternalError, NotFoundError}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.query.PublicProperty
import org.thp.scalligraph.services.RichElement
import shapeless.HNil

final class ScalarSteps[T: ClassTag](raw: GremlinScala[T])(implicit val graph: Graph)
    extends Steps[T, T, HNil](raw)(SingleMapping[T, T]())
    with ScalliSteps[T, T, ScalarSteps[T]] {

  lazy val logger = Logger(getClass)

  override def newInstance(raw: GremlinScala[T]): ScalarSteps[T] =
    new ScalarSteps[T](raw)

  override def map[A: ClassTag](f: T ⇒ A): ScalarSteps[A] = new ScalarSteps(raw.map(f))

  override def toList(): List[T] = {
    logger.info(s"Execution of $raw")
    super.toList()
  }

  override def head(): T = {
    logger.info(s"Execution of $raw")
    super.head()
  }

  override def getOrFail(): T = headOption().getOrElse {
    val typeName = implicitly[ClassTag[T]].runtimeClass.getSimpleName
    throw NotFoundError(s"$typeName not found")
  }
}

object ScalarSteps {
  def apply[T: ClassTag](raw: GremlinScala[T])(implicit graph: Graph): ScalarSteps[T] =
    new ScalarSteps[T](raw)
}

trait ScalliSteps[EndDomain, EndGraph, ThisStep <: AnyRef] { _: ThisStep ⇒
  def graph: Graph
  override def clone(): ThisStep = newInstance(raw.clone())
  def newInstance(raw: GremlinScala[EndGraph]): ThisStep
  val raw: GremlinScala[EndGraph]
  def toList(): Seq[EndDomain]
  def head(): EndDomain
  def headOption(): Option[EndDomain]

  def getOrFail(): EndDomain
  def count(): Long
  def sort(orderBys: OrderBy[_]*): ThisStep                        = newInstance(raw.order(orderBys: _*))
  def where(f: GremlinScala[EndGraph] ⇒ GremlinScala[_]): ThisStep = newInstance(raw.where(f))
  def map[NewEndDomain: ClassTag](f: EndDomain ⇒ NewEndDomain): ScalarSteps[NewEndDomain]
  def get[A](authContext: Option[AuthContext], property: PublicProperty[_, A]): ScalarSteps[A] = {
    val fn = property.get(authContext).asInstanceOf[Seq[GremlinScala[EndGraph] ⇒ GremlinScala[A]]]
    if (fn.size == 1)
      ScalarSteps(fn.head.apply(raw))(ClassTag(property.mapping.graphTypeClass), graph)
    else
      ScalarSteps(raw.coalesce(fn: _*))(ClassTag(property.mapping.graphTypeClass), graph)
  }
}

abstract class ElementSteps[E <: Product: ru.TypeTag, EndGraph <: Element, ThisStep <: ElementSteps[E, EndGraph, ThisStep]](
    raw: GremlinScala[EndGraph])(implicit val db: Database, graph: Graph)
    extends Steps[E with Entity, EndGraph, HNil](raw)(db.getModel[E].converter(db, graph).asInstanceOf[Converter.Aux[E with Entity, EndGraph]])
    with ScalliSteps[E with Entity, EndGraph, ThisStep] { _: ThisStep ⇒

  lazy val logger = Logger(getClass)
  override def toList(): List[E with Entity] = {
    logger.info(s"Execution of $raw")
    super.toList()
  }

  override def headOption(): Option[E with Entity] = {
    logger.info(s"Execution of $raw")
    super.headOption()
  }

  override def getOrFail(): E with Entity = headOption().getOrElse(throw NotFoundError(s"${ru.typeOf[E]} not found"))

  override def map[T: ClassTag](f: E with Entity ⇒ T): ScalarSteps[T] = {
    implicit val const: Constructor.Aux[T, HNil, T, ScalarSteps[T]] =
      new Constructor[T, HNil] {
        type GraphType = T
        type StepsType = ScalarSteps[T]
        def apply(raw: GremlinScala[GraphType]): StepsType = new ScalarSteps[T](raw)
      }
    implicit val identityConverter: Converter.Aux[T, T] = Converter.identityConverter[T]
    map[T, T, ScalarSteps[T]](f)
  }

//  def filter(f: EntityFilter): ThisStep = newInstance(f(raw))

  def order(orderBys: List[OrderBy[_]]): ThisStep = newInstance(raw.order(orderBys: _*))

  class ValueMap(m: JMap[String, _]) {
    def get[A](name: String): A = m.get(name).asInstanceOf[A]
  }

  object ValueMap {
    def unapply(m: JMap[String, _]): Option[ValueMap] = Some(new ValueMap(m))
  }

  def onlyOneOfEntity[A <: Product: ru.TypeTag](elements: JList[_ <: Element]): A with Entity = {
    val size = elements.size
    if (size == 1) elements.get(0).as[A]
    else if (size > 1) throw InternalError(s"Too many ${ru.typeOf[A]} in result ($size found)")
    else throw InternalError(s"No ${ru.typeOf[A]} found")
  }

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
}

abstract class BaseVertexSteps[E <: Product: ru.TypeTag, ThisStep <: BaseVertexSteps[E, ThisStep]](raw: GremlinScala[Vertex])(
    implicit db: Database,
    val graph: Graph)
    extends ElementSteps[E, Vertex, ThisStep](raw) { _: ThisStep ⇒
}

final class VertexSteps[E <: Product: ru.TypeTag](raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph)
    extends BaseVertexSteps[E, VertexSteps[E]](raw) {
  override def newInstance(raw: GremlinScala[Vertex]): VertexSteps[E] = new VertexSteps[E](raw)
}

abstract class BaseEdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product, ThisStep <: BaseEdgeSteps[E, FROM, TO, ThisStep]](
    raw: GremlinScala[Edge])(implicit db: Database, val graph: Graph)
    extends ElementSteps[E, Edge, ThisStep](raw) { _: ThisStep ⇒
}

final class EdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product](raw: GremlinScala[Edge])(implicit db: Database, graph: Graph)
    extends BaseEdgeSteps[E, FROM, TO, EdgeSteps[E, FROM, TO]](raw) {
  override def newInstance(raw: GremlinScala[Edge]): EdgeSteps[E, FROM, TO] = new EdgeSteps[E, FROM, TO](raw)
}
