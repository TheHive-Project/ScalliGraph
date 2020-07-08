package org.thp.scalligraph.steps

import gremlin.scala.{__, GremlinScala}
import gremlin.scala.dsl.Converter
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.thp.scalligraph.models.{Mapping, UniMapping}
import play.api.Logger

import scala.reflect.{classTag, ClassTag}
import scala.util.Try

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
  def apply[T: ClassTag](raw: GremlinScala[T]) = new Traversal[T, T](raw, identity[T])
}

class UntypedTraversal(val traversal: GraphTraversal[_, _]) {
  def onGraphTraversal[A, B](f: GraphTraversal[A, B] => GraphTraversal[_, _]) = new UntypedTraversal(f(traversal.asInstanceOf[GraphTraversal[A, B]]))
}
class Traversal[+D, G: ClassTag](val raw: GremlinScala[G], conv: G => D) {
  def typeName: String                                                                = classTag[G].runtimeClass.getSimpleName
  def deepRaw: GraphTraversal[_, G]                                                   = raw.traversal
  def onRaw(f: GremlinScala[G] => GremlinScala[G]): Traversal[D, G]                   = new Traversal[D, G](f(raw), conv)
  def onRawMap[D2, G2: ClassTag](f: GremlinScala[G] => GremlinScala[G2], c: G2 => D2) = new Traversal[D2, G2](f(raw), c)
  def onDeepRaw(f: GraphTraversal[_, G] => GraphTraversal[_, G])                      = new Traversal(new GremlinScala[G](f(raw.traversal)), conv)
  def onDeepRawMap[D2, G2: ClassTag](f: GraphTraversal[_, G] => GraphTraversal[_, G2], c: G2 => D2) =
    new Traversal(new GremlinScala[G2](f(raw.traversal)), c)
  def start = new Traversal[D, G](__[G], conv)

//  def mapping: UniMapping[D]
//  def converter: Converter.Aux[D, G] = mapping

  def toDomain(g: G): D = conv(g)
//  def cast[DD, GG](m: Mapping[_, DD, GG]): Option[Traversal[DD, GG]] =
//    if (m.isCompatibleWith(mapping)) Some(new Traversal(raw.asInstanceOf[GremlinScala[GG]], m))
//    else None

  def asNumber: Try[Traversal[Number, Number]]                  = ???
  def changeMapping[DD](m: Mapping[_, DD, G]): Traversal[DD, G] = new Traversal(raw, m.toDomain)
  override def clone(): Traversal[D, G]                         = new Traversal[D, G](raw.clone, conv)
}
