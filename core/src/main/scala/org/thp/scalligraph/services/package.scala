package org.thp.scalligraph

import gremlin.scala.{Edge, Element, Graph, GremlinScala, Vertex}
import org.thp.scalligraph.models.{Database, Entity, Schema}

import scala.reflect.runtime.{universe => ru}

package object services {
  implicit class RichElement(e: Element)(implicit db: Database) {

    def as[E <: Product: ru.TypeTag]: E with Entity = {
      val model = db.getModel[E]
      model.toDomain(e.asInstanceOf[model.ElementType])
    }

    def asEntity(implicit db: Database, schema: Schema, graph: Graph): Entity = {
      val model = schema
        .getModel(e.label())
        .getOrElse(
          throw InternalError(
            s"No model found in ${schema.getClass.getSimpleName} for element $e with label ${e.label} (available models are ${schema.modelList.map(_.label).mkString(",")})"
          )
        )
      model
        .converter(db, graph)
        .toDomain(e.asInstanceOf[model.ElementType])
    }
  }

  implicit class RichVertexGremlinScala[End <: Vertex](val g: GremlinScala[End]) {
    def outTo[E <: Product: ru.TypeTag]: GremlinScala.Aux[Vertex, g.Labels] = g.out(ru.typeOf[E].typeSymbol.name.toString)
    def outToE[E <: Product: ru.TypeTag]: GremlinScala.Aux[Edge, g.Labels]  = g.outE(ru.typeOf[E].typeSymbol.name.toString)
    def inTo[E <: Product: ru.TypeTag]: GremlinScala.Aux[Vertex, g.Labels]  = g.in(ru.typeOf[E].typeSymbol.name.toString)
    def inToE[E <: Product: ru.TypeTag]: GremlinScala.Aux[Edge, g.Labels]   = g.inE(ru.typeOf[E].typeSymbol.name.toString)
  }
}
