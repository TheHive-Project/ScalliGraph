package org.thp.scalligraph.services

import java.util.Date

import gremlin.scala._
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

abstract class VertexSrv[V <: Product: ru.TypeTag](implicit db: Database, val model: Model.Vertex[V]) extends ElementSrv[V, Vertex] {
  override def initSteps(implicit graph: Graph): Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] =
    new Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]](db.labelFilter(model)(graph.V), model.converter)

  override def getByIds(ids: String*)(implicit graph: Graph): Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] =
    if (ids.isEmpty) new Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]](graph.inject(), model.converter)
    else new Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]](db.labelFilter(model)(graph.V(ids: _*)), model.converter)

  def get(vertex: Vertex)(implicit graph: Graph): Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] =
    new Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]](db.labelFilter(model)(graph.V(vertex)), model.converter)

  def getOrFail(id: String)(implicit graph: Graph): Try[V with Entity] =
    get(id)
      .headOption()
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} $id not found")))(Success.apply)

  def getOrFail(vertex: Vertex)(implicit graph: Graph): Try[V with Entity] =
    new Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]](db.labelFilter(model)(graph.V(vertex)), model.converter)
      .headOption()
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} ${vertex.id()} not found")))(Success.apply)

  def createEntity(e: V)(implicit graph: Graph, authContext: AuthContext): Try[V with Entity] =
    Success(db.createVertex[V](graph, authContext, model, e))

  def exists(e: V)(implicit graph: Graph): Boolean = false

  def update(
      steps: Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] => Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]],
      propertyUpdaters: Seq[PropertyUpdater]
  )(implicit graph: Graph, authContext: AuthContext): Try[(Traversal[V, Vertex, Converter[V with Entity, Vertex]], JsObject)] =
    update(initSteps, propertyUpdaters)

  def update(
      steps: Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]],
      propertyUpdaters: Seq[PropertyUpdater]
  )(implicit graph: Graph, authContext: AuthContext): Try[(Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]], JsObject)] = {
    val myClone = steps.clone()
    logger.debug(s"Execution of ${steps.raw} (update)")
    steps
      .raw
      .headOption()
      .fold[Try[(Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]], JsObject)]](Failure(NotFoundError(s"${steps} not found"))) {
        vertex =>
          logger.trace(s"Update ${vertex.id()} by ${authContext.userId}")
          propertyUpdaters
            .toTry(u => u(vertex, db, graph, authContext))
            .map { o =>
              db.updatedAtMapping.setProperty(vertex, "_updatedAt", Some(new Date))
              db.updatedByMapping.setProperty(vertex, "_updatedBy", Some(authContext.userId))
              myClone -> o.reduceOption(_ ++ _).getOrElse(JsObject.empty)
            }
      }
  }
}
