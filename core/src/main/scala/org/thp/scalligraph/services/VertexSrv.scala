package org.thp.scalligraph.services

import java.util.Date

import gremlin.scala._
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

abstract class VertexSrv[V <: Product: ru.TypeTag, S <: VertexSteps[V]](implicit db: Database) extends ElementSrv[V, S] {

  override val model: Model.Vertex[V] = db.getVertexModel[V]

  def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): S

  override def initSteps(implicit graph: Graph): S = steps(db.labelFilter(model)(graph.V))

  override def getByIds(ids: String*)(implicit graph: Graph): S =
    if (ids.isEmpty) steps(graph.inject())
    else steps(db.labelFilter(model)(graph.V(ids: _*)))

  def get(vertex: Vertex)(implicit graph: Graph): S = steps(db.labelFilter(model)(graph.V(vertex)))

  def getOrFail(id: String)(implicit graph: Graph): Try[V with Entity] =
    get(id)
      .headOption()
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} $id not found")))(Success.apply)

  def getOrFail(vertex: Vertex)(implicit graph: Graph): Try[V with Entity] =
    steps(db.labelFilter(model)(graph.V(vertex)))
      .headOption()
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} ${vertex.id()} not found")))(Success.apply)

  def createEntity(e: V)(implicit graph: Graph, authContext: AuthContext): Try[V with Entity] =
    Success(db.createVertex[V](graph, authContext, model, e))

  def exists(e: V)(implicit graph: Graph): Boolean = false

  def update(steps: S => S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] =
    update(steps(initSteps), propertyUpdaters)

  def update(steps: S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] = {
    val myClone = steps.newInstance().asInstanceOf[S]
    logger.debug(s"Execution of ${steps.raw} (update)")
    steps.raw.headOption().fold[Try[(S, JsObject)]](Failure(NotFoundError(s"${steps.typeName} not found"))) { vertex =>
      logger.trace(s"Update ${vertex.id()} by ${authContext.userId}")
      val db = steps.db
      propertyUpdaters
        .toTry(u => u(vertex, db, steps.graph, authContext))
        .map { o =>
          db.setOptionProperty(vertex, "_updatedAt", Some(new Date), db.updatedAtMapping)
          db.setOptionProperty(vertex, "_updatedBy", Some(authContext.userId), db.updatedByMapping)
          myClone -> o.reduceOption(_ ++ _).getOrElse(JsObject.empty)
        }
    }
  }
}
