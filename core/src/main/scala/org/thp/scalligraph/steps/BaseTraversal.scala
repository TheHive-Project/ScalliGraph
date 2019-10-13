package org.thp.scalligraph.steps

import java.util.{List => JList}

import gremlin.scala.GremlinScala
import gremlin.scala.dsl.Converter
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.models.{Mapping, UniMapping}
import play.api.Logger

import scala.reflect.{classTag, ClassTag}

trait TraversalBuilder[T <: BaseTraversal] {
  def newInstance(raw: GremlinScala[_]): T
}

object TraversalBuilder {
  implicit def apply[T <: BaseTraversal: ClassTag]: TraversalBuilder[T] =
    (raw: GremlinScala[_]) =>
      classTag[T]
        .runtimeClass
        .getConstructor(classOf[GremlinScala[_]])
        .newInstance(raw)
        .asInstanceOf[T]
}

trait BaseTraversal {
  type EndDomain
  type EndGraph
  lazy val logger = Logger(getClass)
  def typeName: String
  def newInstance(newRaw: GremlinScala[EndGraph]): BaseTraversal
  def newInstance(): BaseTraversal
  def converter: Converter.Aux[EndDomain, EndGraph]
  val raw: GremlinScala[EndGraph]

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

trait TraversalGraph[G] extends BaseTraversal {
  override type EndGraph = G
}

trait TraversalLike[D, G] extends TraversalGraph[G] {
  override type EndDomain = D
  override def typeName: String
  override def newInstance(newRaw: GremlinScala[EndGraph]): TraversalLike[D, G]
  override def newInstance(): TraversalLike[D, G]
  override def converter: Converter.Aux[EndDomain, EndGraph]
  override val raw: GremlinScala[EndGraph]
}

object Traversal {
  def apply[T: ClassTag](raw: GremlinScala[T]) = new Traversal[T, T](raw, UniMapping.identity[T])
}

class Traversal[D, G](val raw: GremlinScala[G], mapping: Mapping[_, D, G]) extends TraversalLike[D, G] with TraversalGraph[G] {
  override def typeName: String                                      = mapping.domainTypeClass.getSimpleName
  override def newInstance(newRaw: GremlinScala[G]): Traversal[D, G] = new Traversal[D, G](newRaw, mapping)
  override def newInstance(): Traversal[D, G]                        = new Traversal[D, G](raw, mapping)
  override def converter: Converter.Aux[D, G]                        = mapping

  def cast[DD, GG](m: Mapping[_, DD, GG]): Option[Traversal[DD, GG]] =
    if (m.isCompatibleWith(mapping)) Some(new Traversal(raw.asInstanceOf[GremlinScala[GG]], m))
    else None
}
