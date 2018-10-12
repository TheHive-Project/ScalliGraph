package org.thp.scalligraph.models

import java.util.{List ⇒ JList, Map ⇒ JMap}

import gremlin.scala._
import gremlin.scala.dsl._
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.query.PublicProperty
import org.thp.scalligraph.services.RichElement
import shapeless.HNil

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe ⇒ ru}

final class ScalarSteps[T: ClassTag](raw: GremlinScala[T])
    extends Steps[T, T, HNil](raw)(SingleMapping[T, T]())
    with ScalliSteps[T, T, ScalarSteps[T]] {

  override def newInstance(raw: GremlinScala[T]): ScalarSteps[T] =
    new ScalarSteps[T](raw)

  override def map[A: ClassTag](f: T ⇒ A): ScalarSteps[A] = new ScalarSteps(raw.map(f))
}

object ScalarSteps {
  def apply[T: ClassTag](raw: GremlinScala[T]): ScalarSteps[T] =
    new ScalarSteps[T](raw)
}

trait ScalliSteps[EndDomain, EndGraph, ThisStep <: AnyRef] { _: ThisStep ⇒
  override def clone(): ThisStep = newInstance(raw.clone())
  def newInstance(raw: GremlinScala[EndGraph]): ThisStep
  val raw: GremlinScala[EndGraph]
  def toList: Seq[EndDomain]
  def head: EndDomain
  def headOption: Option[EndDomain]
  def count: Long                                                  = raw.count().head
  def sort(orderBys: OrderBy[_]*): ThisStep                        = newInstance(raw.order(orderBys: _*))
  def where(f: GremlinScala[EndGraph] ⇒ GremlinScala[_]): ThisStep = newInstance(raw.where(f))
  def map[NewEndDomain: ClassTag](f: EndDomain ⇒ NewEndDomain): ScalarSteps[NewEndDomain]
  def get[A](authContext: Option[AuthContext], property: PublicProperty[_, A]): ScalarSteps[A] = {
    val fn = property.fn(authContext).asInstanceOf[Seq[GremlinScala.Aux[EndGraph, HNil] ⇒ GremlinScala[A]]]
    ScalarSteps(raw.coalesce(fn: _*))(ClassTag(property.mapping.graphTypeClass))
  }
}

abstract class ElementSteps[E <: Product: ru.TypeTag, EndGraph <: Element, ThisStep <: ElementSteps[E, EndGraph, ThisStep]](
    raw: GremlinScala[EndGraph])(implicit db: Database)
    extends Steps[E with Entity, EndGraph, HNil](raw)(
      db.getModel[E].converter(db, raw.traversal.asAdmin.getGraph.get).asInstanceOf[Converter.Aux[E with Entity, EndGraph]])
    with ScalliSteps[E with Entity, EndGraph, ThisStep] { _: ThisStep ⇒

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

abstract class BaseVertexSteps[E <: Product: ru.TypeTag, ThisStep <: BaseVertexSteps[E, ThisStep]](raw: GremlinScala[Vertex])(implicit db: Database)
    extends ElementSteps[E, Vertex, ThisStep](raw) { _: ThisStep ⇒
}

final class VertexSteps[E <: Product: ru.TypeTag](raw: GremlinScala[Vertex])(implicit db: Database) extends BaseVertexSteps[E, VertexSteps[E]](raw) {
  override def newInstance(raw: GremlinScala[Vertex]): VertexSteps[E] = new VertexSteps[E](raw)
}

abstract class BaseEdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product, ThisStep <: BaseEdgeSteps[E, FROM, TO, ThisStep]](
    raw: GremlinScala[Edge])(implicit db: Database)
    extends ElementSteps[E, Edge, ThisStep](raw) { _: ThisStep ⇒
}

final class EdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product](raw: GremlinScala[Edge])(implicit db: Database)
    extends BaseEdgeSteps[E, FROM, TO, EdgeSteps[E, FROM, TO]](raw) {
  override def newInstance(raw: GremlinScala[Edge]): EdgeSteps[E, FROM, TO] = new EdgeSteps[E, FROM, TO](raw)
}
