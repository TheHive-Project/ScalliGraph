package org.thp.scalligraph.services

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

import play.api.libs.json.JsObject

import gremlin.scala._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater

abstract class VertexSrv[V <: Product: ru.TypeTag, S <: BaseVertexSteps[V, S]](implicit db: Database) extends ElementSrv[V, S] {

  override val model: Model.Vertex[V] = db.getVertexModel[V]

  def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): S

  def initSteps(implicit graph: Graph): S = steps(db.vertexStep(graph, model))

  def get(vertex: Vertex)(implicit graph: Graph): S = initSteps.get(vertex)

  def create(e: V)(implicit graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, e)

  val initialValues: Seq[V] = Nil

  def getInitialValues: Seq[InitialValue[V]] = initialValues.map(v => InitialValue(model, v))

  def createInitialValues()(implicit graph: Graph, authContext: AuthContext): Unit = initialValues.foreach(create)

  def update(steps: S => S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] =
    update(steps(initSteps), propertyUpdaters)

  def update(steps: S, propertyUpdaters: Seq[PropertyUpdater])(implicit graph: Graph, authContext: AuthContext): Try[(S, JsObject)] =
    steps.updateProperties(propertyUpdaters)
}
