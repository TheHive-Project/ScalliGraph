package org.thp.scalligraph.steps

import gremlin.scala.GremlinScala
import java.lang.{Long => JLong}
import java.util.{List => JList}
import scala.collection.JavaConverters._
import gremlin.scala.dsl.Converter
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, DefaultGraphTraversal, GraphTraversal}
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

abstract class GraphConverter[+D, -G] extends (G => D) {
  val isIdentity: Boolean       = false
  def untypedApply(g: Any): Any = apply(g.asInstanceOf[G])
}
object GraphConverter {
  type any = GraphConverter[Nothing, Any]
  def identity[A]: GraphConverter[A, A] = new GraphConverter[A, A] {
    override def apply(v: A): A      = v
    override val isIdentity: Boolean = true
  }
  def long: GraphConverter[Long, JLong] = _.toLong
}

//class IdentityGraphConverterMapper[-F <: GraphConverter[_, _], +T <: GraphConverter[_, _]] extends GraphConverterMapper[F, T] {
//  override def apply(c: F): T = c.asInstanceOf[T]
//}
//
//object IdentityGraphConverterMapper {
//  implicit def default[A]: IdentityGraphConverterMapper[A, A] = new IdentityGraphConverterMapper[A, A]
//}
//
//class ToIdentityGraphConverterMapper[-F <: GraphConverter[_, _], A, B] extends GraphConverterMapper[F, GraphConverter[A, B]] {
//  override def apply(c: F): T = GraphConverter.identity[T]
//}
abstract class GraphConverterMapper[-F <: GraphConverter[_, _], +T <: GraphConverter[_, _]] extends (F => T) {
  def untypedApply(from: Any): GraphConverter[_, _] = apply(from.asInstanceOf[F])
}
object GraphConverterMapper {
  def apply[D, G](f: G => D): GraphConverterMapper[_, GraphConverter[D, G]] = _ => f(_)
  def identity[A <: GraphConverter[_, _]]: GraphConverterMapper[A, A]       = Predef.identity(_)
  def toIdentity[A]: GraphConverterMapper[A, _]                             = _ => GraphConverter.identity
  def toList: GraphConverterMapper[_, _] =
    (conv: GraphConverter[_, _]) =>
      if (conv.isIdentity) (_: Any).asInstanceOf[JList[_]].asScala
      else (_: Any).asInstanceOf[JList[Any]].asScala.map(conv.asInstanceOf[Any => Any])
}

object Traversal {
  def apply[T: ClassTag](raw: GremlinScala[T]) = new Traversal[T, T](raw, identity[T])
}

class UntypedTraversal(val traversal: GraphTraversal[_, _], val converter: GraphConverter[_, _]) {
  def onGraphTraversal[A, B](f: GraphTraversal[A, B] => GraphTraversal[_, _], convMap: GraphConverterMapper[_, _]) =
    new UntypedTraversal(f(traversal.asInstanceOf[GraphTraversal[A, B]]), convMap.untypedApply(converter))
  override def clone(): UntypedTraversal =
    traversal match {
      case dgt: DefaultGraphTraversal[_, _] => new UntypedTraversal(dgt.clone, converter)
    }
}

object UntypedTraversal {
  def start: UntypedTraversal = new UntypedTraversal(__.start(), GraphConverter.identity)
}

class Traversal[+D, G: ClassTag](val raw: GremlinScala[G], val converter: GraphConverter[D, G]) {
  def typeName: String                                              = classTag[G].runtimeClass.getSimpleName
  def deepRaw: GraphTraversal[_, G]                                 = raw.traversal
  def onRaw(f: GremlinScala[G] => GremlinScala[G]): Traversal[D, G] = new Traversal[D, G](f(raw), converter)
  def onRawMap[D2, G2: ClassTag](
      f: GremlinScala[G] => GremlinScala[G2],
      convMap: GraphConverterMapper[GraphConverter[D, G], GraphConverter[D2, G2]]
  )                                                              = new Traversal[D2, G2](f(raw), convMap(converter))
  def onDeepRaw(f: GraphTraversal[_, G] => GraphTraversal[_, G]) = new Traversal(new GremlinScala[G](f(raw.traversal)), converter)
  def onDeepRawMap[D2, G2: ClassTag](
      f: GraphTraversal[_, G] => GraphTraversal[_, G2],
      convMap: GraphConverterMapper[GraphConverter[D, G], GraphConverter[D2, G2]]
  ): Traversal[D2, G2] =
    new Traversal(new GremlinScala[G2](f(raw.traversal)), convMap(converter))
  def map[DD](f: D => DD): Traversal[DD, G] = new Traversal(raw, g => converter.andThen(f).apply(g))
  def start                                 = new Traversal[D, G](gremlin.scala.__[G], converter)

//  def mapping: UniMapping[D]
//  def converter: Converter.Aux[D, G] = mapping

  def toDomain(g: G): D = converter(g)
//  def cast[DD, GG](m: Mapping[_, DD, GG]): Option[Traversal[DD, GG]] =
//    if (m.isCompatibleWith(mapping)) Some(new Traversal(raw.asInstanceOf[GremlinScala[GG]], m))
//    else None

  def asNumber: Try[Traversal[Number, Number]] = ???
//  def changeMapping[DD](m: Mapping[_, DD, G]): Traversal[DD, G] = new Traversal(raw, m.toDomain)
  override def clone(): Traversal[D, G] = new Traversal[D, G](raw.clone, converter)
}
