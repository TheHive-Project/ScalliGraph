package org.thp.scalligraph.steps

import java.util.{List => JList, Map => JMap}

import gremlin.scala._
import gremlin.scala.dsl._
import org.thp.scalligraph.models._

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

case class PagedResult[R](result: Seq[R], totalSize: Option[Long]) {
  def map[T](f: R => T): PagedResult[T] = PagedResult(result.map(f), totalSize)
}

class ValueMap(m: Map[String, Seq[AnyRef]]) {
  def get[A](name: String): A = m(name).head.asInstanceOf[A]
}

object ValueMap {

  def unapply(m: JMap[_, _]): Option[ValueMap] =
    Some(new ValueMap(m.asScala.map { case (k, v) => k.toString -> v.asInstanceOf[JList[AnyRef]].asScala }.toMap))
}

object IdMapping extends Mapping[String, String, AnyRef] {
  override val cardinality: MappingCardinality.Value     = MappingCardinality.single
  override def toGraphOpt(d: String): Option[AnyRef]     = Some(d)
  override def toDomain(g: AnyRef): String               = g.toString
  override val isReadonly: Boolean                       = true
  override def readonly: Mapping[String, String, AnyRef] = this
}

abstract class BaseElementSteps extends BaseTraversal {
  override type EndGraph <: Element
  val db: Database
  val graph: Graph
}

abstract class BaseVertexSteps extends BaseElementSteps with TraversalGraph[Vertex] {
  override def newInstance(newRaw: GremlinScala[Vertex]): BaseVertexSteps
  override def newInstance(): BaseVertexSteps
}

class VertexSteps[E <: Product: ru.TypeTag](val raw: GremlinScala[Vertex])(implicit val db: Database, val graph: Graph)
    extends BaseVertexSteps
    with TraversalLike[E with Entity, Vertex] {
  override type EndGraph = Vertex
  lazy val model: Model.Vertex[E] = db.getVertexModel[E]

  override def converter: Converter.Aux[E with Entity, Vertex] = model.converter(db, graph)

  override def newInstance(newRaw: GremlinScala[Vertex]): VertexSteps[E] = new VertexSteps[E](newRaw)
  override def newInstance(): VertexSteps[E]                             = new VertexSteps[E](raw.clone())

  override def typeName: String = ru.typeOf[E].toString
}

abstract class BaseEdgeSteps extends BaseElementSteps with TraversalGraph[Edge] {
  override def newInstance(newRaw: GremlinScala[Edge]): BaseEdgeSteps
  override def newInstance(): BaseEdgeSteps
}

class EdgeSteps[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product](val raw: GremlinScala[Edge])(
    implicit val db: Database,
    val graph: Graph
) extends BaseEdgeSteps
    with TraversalLike[E with Entity, Edge] {
  override type EndGraph  = Edge
  override type EndDomain = E with Entity
  lazy val model: Model.Edge[E, FROM, TO] = db.getEdgeModel[E, FROM, TO]

  override def converter: Converter.Aux[E with Entity, Edge] = db.getModel[E].converter(db, graph).asInstanceOf[Converter.Aux[E with Entity, Edge]]

  override def newInstance(newRaw: GremlinScala[Edge]): EdgeSteps[E, FROM, TO] = new EdgeSteps[E, FROM, TO](newRaw)
  override def newInstance(): EdgeSteps[E, FROM, TO]                           = new EdgeSteps[E, FROM, TO](raw.clone())

  override def typeName: String = ru.typeOf[E].toString
}
