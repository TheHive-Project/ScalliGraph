package org.thp.scalligraph.services

import gremlin.scala._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._

import scala.reflect.runtime.{universe ⇒ ru}

abstract class VertexSrv[V <: Product: ru.TypeTag, S <: BaseVertexSteps[V, S]](implicit db: Database) extends ElementSrv[V, S] {

  override val model: Model.Vertex[V] = db.getVertexModel[V]

  def steps(raw: GremlinScala[Vertex]): S

  def initSteps(implicit graph: Graph): S = steps(graph.V.hasLabel(model.label))

  override def get(id: String)(implicit graph: Graph): S = steps(graph.V().has(Key("_id") of id))

  def create(e: V)(implicit graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, e)

  val initialValues: Seq[V] = Nil

  def createInitialValues()(implicit graph: Graph, authContext: AuthContext): Unit = initialValues.foreach(create)
}
