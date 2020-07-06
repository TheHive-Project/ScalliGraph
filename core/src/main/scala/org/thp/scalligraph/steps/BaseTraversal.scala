package org.thp.scalligraph.steps

import gremlin.scala.GremlinScala
import gremlin.scala.dsl.Converter
import org.thp.scalligraph.models.{Mapping, UniMapping}
import play.api.Logger

import scala.reflect.{classTag, ClassTag}

//trait TraversalBuilder[T <: BaseTraversal] {
//  def newInstance(raw: GremlinScala[_]): T
//}
//
//object TraversalBuilder {
//  implicit def apply[T <: BaseTraversal: ClassTag]: TraversalBuilder[T] =
//    (raw: GremlinScala[_]) =>
//      classTag[T]
//        .runtimeClass
//        .getConstructor(classOf[GremlinScala[_]])
//        .newInstance(raw)
//        .asInstanceOf[T]
//}
//
//trait BaseTraversal {
//  type EndDomain
//  type EndGraph
//  lazy val logger: Logger = Logger(getClass)
//  def typeName: String
//  def newInstance(newRaw: GremlinScala[EndGraph]): BaseTraversal
//  def newInstance(): BaseTraversal
//  def converter: Converter.Aux[EndDomain, EndGraph]
//  val raw: GremlinScala[EndGraph]
//}

//trait TraversalGraph[G] extends BaseTraversal {
//  override type EndGraph = G
//}

//trait TraversalLike[D, G] extends TraversalGraph[G] {
//  override type EndDomain = D
//  override def typeName: String
//  override def newInstance(newRaw: GremlinScala[EndGraph]): TraversalLike[D, G]
//  override def newInstance(): TraversalLike[D, G]
//  override def converter: Converter.Aux[EndDomain, EndGraph]
//  override val raw: GremlinScala[EndGraph]
//}

object Traversal {
  def apply[T: ClassTag](raw: GremlinScala[T]) = new Traversal[T, T](raw, UniMapping.identity[T])
}

class Traversal[+D, G](val raw: GremlinScala[G], mapping: Mapping[_, D, G]) {
  def typeName: String = mapping.domainTypeClass.getSimpleName
//  def mapping: UniMapping[D]
//  def converter: Converter.Aux[D, G] = mapping

  def toDomain(g: G): D = mapping.toDomain(g)
  def cast[DD, GG](m: Mapping[_, DD, GG]): Option[Traversal[DD, GG]] =
    if (m.isCompatibleWith(mapping)) Some(new Traversal(raw.asInstanceOf[GremlinScala[GG]], m))
    else None

  override def clone(): Traversal[D, G] = new Traversal[D, G](raw.clone, mapping)
}
