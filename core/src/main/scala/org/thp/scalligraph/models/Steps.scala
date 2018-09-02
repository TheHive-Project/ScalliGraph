package org.thp.scalligraph.models

import java.util.{List ⇒ JList, Map ⇒ JMap}

import gremlin.scala.dsl.{Steps, _}
import gremlin.scala.{Edge, Element, GremlinScala, Vertex}
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.query.{CompositeFilter, Filter, PredicateFilter}
import org.thp.scalligraph.services.RichElement
import shapeless.HNil

import scala.reflect.runtime.{universe ⇒ ru}

final class ScalarSteps[EndDomain, EndGraph](mapping: Mapping[_, EndDomain, EndGraph], raw: GremlinScala[EndGraph])
    extends Steps[EndDomain, EndGraph, HNil](raw)(mapping)
    with ScalliSteps[EndDomain, EndGraph, ScalarSteps[EndDomain, EndGraph]] {

  override protected def newInstance(raw: GremlinScala[EndGraph]): ScalarSteps[EndDomain, EndGraph] =
    new ScalarSteps[EndDomain, EndGraph](mapping, raw)
}
object ScalarSteps {
  def apply[EndDomain, EndGraph](mapping: Mapping[_, EndDomain, EndGraph], raw: GremlinScala[EndGraph]): ScalarSteps[EndDomain, EndGraph] =
    new ScalarSteps[EndDomain, EndGraph](mapping, raw)
}

//class LeafSteps[E, Labels <: HList](raw: GremlinScala[E]) extends Steps[E, E, Labels](raw)(Converter.identityConverter[E]) with ScalliSteps[E, E, LeafSteps[E, Labels]] {
//
//  override protected def newInstance(raw: GremlinScala[E]): LeafSteps[E, Labels] = new LeafSteps[E, Labels](raw)
//}

trait ScalliSteps[EndDomain, EndGraph, ThisStep <: Steps[EndDomain, EndGraph, _]] { _: ThisStep ⇒
  protected def newInstance(raw: GremlinScala[EndGraph]): ThisStep

  def toList: List[EndDomain]
  def head: EndDomain
  def headOption: Option[EndDomain]
  def count: Long    = raw.count().head
  def sort: ThisStep = newInstance(raw.order())
}

abstract class ElementSteps[E <: Product: ru.TypeTag, EndGraph <: Element, ThisStep <: ElementSteps[E, EndGraph, ThisStep]](
    raw: GremlinScala[EndGraph])(implicit db: Database)
    extends Steps[E with Entity, EndGraph, HNil](raw)(
      db.getModel[E].converter(db, raw.traversal.asAdmin.getGraph.get).asInstanceOf[Converter.Aux[E with Entity, EndGraph]])
    with ScalliSteps[E with Entity, EndGraph, ThisStep] { _: ThisStep ⇒

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

//  /** Aggregate all objects at this point into the given collection, e.g. `mutable.ArrayBuffer.empty[EndDomain]`
//    * Uses eager evaluation (as opposed to `store`() which lazily fills a collection)
//    */
//  def aggregate[NewSteps](into: mutable.Buffer[E with Entity])(implicit constr: Constructor.Aux[E with Entity, HNil, EndGraph, NewSteps]): NewSteps =
//    constr(
//      raw.sideEffect { v ⇒
//        into += converter.toDomain(v)
//      }
//    )
//
//  def filterOnEnd[NewSteps](predicate: E with Entity ⇒ Boolean)(implicit constr: Constructor.Aux[E with Entity, HNil, EndGraph, NewSteps]): NewSteps =
//    constr(
//      raw.filterOnEnd { v: EndGraph ⇒
//        predicate(converter.toDomain(v))
//      }
//    )

//  override def newInstance(raw: GremlinScala[EndGraph]): ElementSteps[E, EndGraph]

  def filter(f: EntityFilter[EndGraph]): ThisStep = newInstance(f(raw))

  val filterHook: PartialFunction[PredicateFilter[EndGraph], Filter[EndGraph]] = PartialFunction.empty

  def filter(filter: Filter[EndGraph]): ThisStep = {
    def convertFilter(f: Filter[EndGraph]): Filter[EndGraph] =
      f match {
        case pf: PredicateFilter[EndGraph] ⇒ filterHook.applyOrElse(pf, identity[Filter[EndGraph]])
        case cf: CompositeFilter[EndGraph] ⇒ cf.updateFilters(cf.filters.map(convertFilter))
      }
    newInstance(convertFilter(filter)(raw))
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
