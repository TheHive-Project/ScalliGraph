package org.thp.scalligraph.services

import gremlin.scala.Graph
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.UpdateOps
import org.thp.scalligraph.models.{Database, ElementSteps, Entity, Model}
import org.thp.scalligraph.{FPath, NotFoundError}

abstract class ElementSrv[E <: Product](implicit db: Database) {
  val model: Model.Base[E]

  def steps(implicit graph: Graph): ElementSteps[E, _, _]

  def get(id: String)(implicit graph: Graph): ElementSteps[E, _, _]

  def getOrFail(id: String)(implicit graph: Graph): E with Entity = get(id).headOption.getOrElse(throw NotFoundError(s"${model.label} $id not found"))

  def count(implicit graph: Graph): Long = steps.count

  def update(id: String, path: String, value: Any)(implicit graph: Graph, authContext: AuthContext): Unit =
    update(id, Map(FPath(path) → UpdateOps.SetAttribute(value)))

  def update(id: String, fields: Map[FPath, UpdateOps.Type])(implicit graph: Graph, authContext: AuthContext): Unit =
    db.update(graph, authContext, model, id, fields)
}
