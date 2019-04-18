package org.thp.scalligraph

import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala.{Edge, Element, Graph, GremlinScala, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.thp.scalligraph.models.{Database, Entity, Schema}

package object services {
  implicit class RichElement(e: Element)(implicit db: Database) {
    def as[E <: Product: ru.TypeTag]: E with Entity = {
      val model = db.getModel[E]
      model.toDomain(e.asInstanceOf[model.ElementType])
    }

    def asEntity(implicit db: Database, schema: Schema, graph: Graph): Entity = {
      val model = schema
        .getModel(e.label())
        .getOrElse(throw InternalError(s"No model found in ${schema.getClass.getSimpleName} for element $e with label ${e.label}"))
      model
        .converter(db, graph)
        .toDomain(e.asInstanceOf[model.ElementType])
    }
  }

  implicit class RichGraphTraversal[G, D](g: GraphTraversal[G, D])(implicit db: Database) {
//    def outTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex] = g.out(db.getModel[E].label)
//    def outToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]  = g.outE(db.getModel[E].label)
//    def inTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex]  = g.in(db.getModel[E].label)
//    def inToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]   = g.inE(db.getModel[E].label)
    def outTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex] = g.out(ru.typeOf[E].typeSymbol.name.toString)
    def outToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]  = g.outE(ru.typeOf[E].typeSymbol.name.toString)
    def inTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex]  = g.in(ru.typeOf[E].typeSymbol.name.toString)
    def inToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]   = g.inE(ru.typeOf[E].typeSymbol.name.toString)
  }

  implicit class RichGremlinScala[End <: Vertex](val g: GremlinScala[End])(implicit db: Database) {
//    def outTo[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Vertex] = g.out(db.getModel[E].label)
//    def outToE[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Edge]  = g.outE(db.getModel[E].label)
//    def inTo[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Vertex]  = g.in(db.getModel[E].label)
//    def inToE[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Edge]   = g.inE(db.getModel[E].label)
    def outTo[E <: Product: ru.TypeTag]: GremlinScala.Aux[Vertex, g.Labels] = g.out(ru.typeOf[E].typeSymbol.name.toString)
    def outToE[E <: Product: ru.TypeTag]: GremlinScala.Aux[Edge, g.Labels]  = g.outE(ru.typeOf[E].typeSymbol.name.toString)
    def inTo[E <: Product: ru.TypeTag]: GremlinScala.Aux[Vertex, g.Labels]  = g.in(ru.typeOf[E].typeSymbol.name.toString)
    def inToE[E <: Product: ru.TypeTag]: GremlinScala.Aux[Edge, g.Labels]   = g.inE(ru.typeOf[E].typeSymbol.name.toString)
  }
}
