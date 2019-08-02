package org.thp.scalligraph.services

import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}
import play.api.libs.json.JsObject
import gremlin.scala._
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater

abstract class VertexSrv[V <: Product: ru.TypeTag, S <: BaseVertexSteps[V, S]](implicit db: Database) extends ElementSrv[V, S] {

  override val model: Model.Vertex[V] = db.getVertexModel[V]

  def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): S

  override def initSteps(implicit graph: Graph): S = steps(db.labelFilter(model)(graph.V))

  override def get(id: String)(implicit graph: Graph): S = steps(db.labelFilter(model)(graph.V(id)))

  def get(vertex: Vertex)(implicit graph: Graph): S = steps(db.labelFilter(model)(graph.V(vertex)))

  def getOrFail(vertex: Vertex)(implicit graph: Graph): Try[V with Entity] =
    steps(db.labelFilter(model)(graph.V(vertex)))
      .headOption()
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} ${vertex.id()} not found")))(Success.apply)

  def create(e: V)(implicit graph: Graph, authContext: AuthContext): Try[V with Entity] =
    Success(db.createVertex[V](graph, authContext, model, e))

  val initialValues: Seq[V] = Nil

  def getInitialValues: Seq[InitialValue[V]] = initialValues.map(v => InitialValue(model, v))

  def update(steps: S => S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] =
    update(steps(initSteps), propertyUpdaters)

  def update(steps: S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] =
    steps.updateProperties(propertyUpdaters)
}
