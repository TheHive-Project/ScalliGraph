package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{__, DefaultGraphTraversal, GraphTraversal}
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Vertex}
import org.thp.scalligraph.models.Entity

import scala.language.existentials

object Traversal {
  type Identity[T] = Traversal[T, T, Converter.Identity[T]]
  type V[T]        = Traversal[T with Entity, Vertex, Converter[T with Entity, Vertex]]
  type E[T]        = Traversal[T with Entity, Edge, Converter[T with Entity, Edge]]
  type Unk         = Traversal[UnkD, UnkG, Converter[UnkD, UnkG]]
  type UnkD        = Any
  type UnkDU       = Any
  type UnkG        = Any

  type Some          = Traversal[D, G, C] forSome { type D; type G; type C <: Converter[D, G] }
  type SomeDomain[D] = Traversal[D, G, C] forSome { type G; type C <: Converter[D, G] }
  type Domain[D]     = Traversal[D, UnkG, Converter[D, UnkG]]

  def V(vertexIds: String*)(implicit graph: Graph) =
    new Traversal[Vertex, Vertex, IdentityConverter[Vertex]](graph.traversal().V(vertexIds: _*), Converter.identity)
  def E(edgeIds: String*)(implicit graph: Graph) =
    new Traversal[Edge, Edge, IdentityConverter[Edge]](graph.traversal().E(edgeIds: _*), Converter.identity)
  def empty[T](implicit graph: Graph) =
    new Traversal[T, T, IdentityConverter[T]](graph.traversal().inject[T](), Converter.identity)
  def union[D, G, C <: Converter[D, G]](t: (Traversal[Vertex, Vertex, IdentityConverter[Vertex]] => Traversal[D, G, C])*)(implicit
      graph: Graph
  ): Traversal[D, G, C] = {
    val traversals: Seq[Traversal[D, G, C]] = t.map(_.apply(Traversal.V()))
    new Traversal[D, G, C](graph.traversal().inject(1).union(traversals.map(_.raw): _*), traversals.head.converter)
  }
}

class Traversal[+D, G, +C <: Converter[D, G]](val raw: GraphTraversal[_, G], val converter: C) {
  def onRaw(f: GraphTraversal[_, G] => GraphTraversal[_, G]): Traversal[D, G, C] =
    new Traversal[D, G, C](f(raw), converter)
  def onRawMap[DD, GG, CC <: Converter[DD, GG]](f: GraphTraversal[_, G] => GraphTraversal[_, GG])(conv: CC): Traversal[DD, GG, CC] =
    new Traversal[DD, GG, CC](f(raw), conv)
  def domainMap[DD](f: D => DD): Traversal[DD, G, Converter[DD, G]] =
    new Traversal[DD, G, Converter[DD, G]](raw, g => converter.andThen(f).apply(g))
  def graphMap[DD, GG, CC <: Converter[DD, GG]](d: G => GG, conv: CC): Traversal[DD, GG, CC] =
    new Traversal[DD, GG, CC](raw.map[GG]((t: Traverser[G]) => d(t.get)), conv)
  def setConverter[DD, CC <: Converter[DD, G]](conv: CC): Traversal[DD, G, CC] = new Traversal[DD, G, CC](raw, conv)
  def start                                                                    = new Traversal[D, G, C](__.start[G](), converter)
  def mapAsNumber(
      f: Traversal[Number, Number, IdentityConverter[Number]] => Traversal[Number, Number, IdentityConverter[Number]]
  ): Traversal[D, G, C] =
    f(this.asInstanceOf[Traversal[Number, Number, IdentityConverter[Number]]]).asInstanceOf[Traversal[D, G, C]]
  def mapAsComparable(f: Traversal[Comparable[_], Comparable[G], _] => Traversal[Comparable[_], Comparable[G], _]): Traversal[D, G, C] =
    f(this.asInstanceOf[Traversal[Comparable[_], Comparable[G], _]]).asInstanceOf[Traversal[D, G, C]]
  override def clone(): Traversal[D, G, C] =
    raw match {
      case dgt: DefaultGraphTraversal[_, G] => new Traversal[D, G, C](dgt.clone, converter)
    }
}
