package org.thp.scalligraph

import gremlin.scala.{Edge, Element, GremlinScala, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.thp.scalligraph.models.{Database, Entity}

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.{universe ⇒ ru}

package object services {
  object Implicits {
    implicit val singleThreadedExecutionContext: ExecutionContext =
      new ExecutionContext {
        override def execute(runnable: Runnable): Unit = runnable.run()
        override def reportFailure(t: Throwable): Unit =
          throw new IllegalStateException("exception in sameThreadExecutionContext", t)
      }
  }

  implicit class RichElement(e: Element)(implicit db: Database) {
    def as[E <: Product: ru.TypeTag]: E with Entity = {
      val model = db.getModel[E]
      model.toDomain(e.asInstanceOf[model.ElementType])
    }
  }

  implicit class RichGraphTraversal[G, D](g: GraphTraversal[G, D])(implicit db: Database) {
    def outTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex] = g.out(db.getModel[E].label)
    def outToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]  = g.outE(db.getModel[E].label)
    def inTo[E <: Product: ru.TypeTag]: GraphTraversal[G, Vertex]  = g.in(db.getModel[E].label)
    def inToE[E <: Product: ru.TypeTag]: GraphTraversal[G, Edge]   = g.inE(db.getModel[E].label)
  }

  implicit class RichGremlinScala[End](val g: GremlinScala[End])(implicit db: Database) {
    def outTo[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Vertex] = g.out(db.getModel[E].label)
    def outToE[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Edge]  = g.outE(db.getModel[E].label)
    def inTo[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Vertex]  = g.in(db.getModel[E].label)
    def inToE[E <: Product: ru.TypeTag](implicit ev: End <:< Vertex): GremlinScala[Edge]   = g.inE(db.getModel[E].label)
  }
}
