package org.thp.scalligraph.services

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

import gremlin.scala._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._

class EdgeSrv[E <: Product: ru.TypeTag, FROM <: Product: ru.TypeTag, TO <: Product: ru.TypeTag](implicit val db: Database)
    extends ElementSrv[E, EdgeSteps[E, FROM, TO]] {
  override val model: Model.Edge[E, FROM, TO] = db.getEdgeModel[E, FROM, TO]

  override def getByIds(ids: String*)(implicit graph: Graph): EdgeSteps[E, FROM, TO] = steps(db.labelFilter(model)(graph.E(ids: _*)))

  def steps(raw: GremlinScala[Edge])(implicit graph: Graph) = new EdgeSteps[E, FROM, TO](raw)

  override def initSteps(implicit graph: Graph): EdgeSteps[E, FROM, TO] = steps(db.labelFilter(model)(graph.E))

  def create(e: E, from: FROM with Entity, to: TO with Entity)(implicit graph: Graph, authContext: AuthContext): Try[E with Entity] =
    Success(db.createEdge[E, FROM, TO](graph, authContext, model, e, from, to))
}
