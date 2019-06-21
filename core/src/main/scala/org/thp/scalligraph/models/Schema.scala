package org.thp.scalligraph.models

import gremlin.scala.Graph
import org.thp.scalligraph.auth.AuthContext

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V) {

  def create()(implicit db: Database, graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, value)
}

trait Schema {
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]                         = Nil
  def getModel(label: String): Option[Model]                      = modelList.find(_.label == label)
  def init(implicit graph: Graph, authContext: AuthContext): Unit = ()
}
