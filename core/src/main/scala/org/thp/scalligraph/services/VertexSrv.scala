package org.thp.scalligraph.services

import gremlin.scala._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._

import scala.reflect.runtime.{universe ⇒ ru}

class VertexSrv[V <: Product: ru.TypeTag](implicit db: Database) extends ElementSrv[V] {
  override val model: Model.Vertex[V] = db.getVertexModel[V]

  def steps(implicit graph: Graph): ElementSteps[V, _, _]     = new VertexSteps[V](graph.V.hasLabel(model.label))
  def steps(raw: GremlinScala[Vertex]): BaseVertexSteps[V, _] = new VertexSteps[V](raw)

  override def get(id: String)(implicit graph: Graph): ElementSteps[V, _, _] = steps(graph.V(id))

  def create(e: V)(implicit graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, e)

  val initialValues: Seq[V] = Nil

  def createInitialValues()(implicit graph: Graph, authContext: AuthContext): Unit = initialValues.foreach(create)
}
