package org.thp.scalligraph.services

import gremlin.scala._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._

import scala.reflect.runtime.{universe ⇒ ru}

class EdgeSrv[E <: Product: ru.TypeTag, FROM <: Product: ru.TypeTag, TO <: Product: ru.TypeTag](implicit val db: Database) extends ElementSrv[E] {
  override val model: Model.Edge[E, FROM, TO] = db.getEdgeModel[E, FROM, TO]

  def steps(implicit graph: Graph): ElementSteps[E, _, _]           = new EdgeSteps[E, FROM, TO](graph.E.hasLabel(model.label))
  def steps(raw: GremlinScala[Edge]): BaseEdgeSteps[E, FROM, TO, _] = new EdgeSteps[E, FROM, TO](raw)

//  override def get(id: String)(implicit graph: Graph): BaseEdgeSteps[E, FROM, TO, _] = steps(graph.E(id))
  override def get(id: String)(implicit graph: Graph): ElementSteps[E, _, _] = steps(graph.E(id))

  def create(e: E, from: FROM with Entity, to: TO with Entity)(implicit graph: Graph, authContext: AuthContext): E with Entity =
    db.createEdge[E, FROM, TO](graph, authContext, model, e, from, to)
}
/*
Error:(8, 7) class EdgeSrv needs to be abstract, since:
it has 2 unimplemented members.
/** As seen from class EdgeSrv, the missing signatures are as follows.
 *  For convenience, these are usable as stub implementations.
 */
  def get(id: String)(implicit graph: gremlin.scala.Graph): org.thp.scalligraph.models.ElementSteps[E, _, _] = ???
  def steps(implicit graph: gremlin.scala.Graph): org.thp.scalligraph.models.ElementSteps[E, _, _] = ???
class EdgeSrv[E <: Product: ru.TypeTag, FROM <: Product: ru.TypeTag, TO <: Product: ru.TypeTag](implicit val db: Database) extends ElementSrv[E] {
 */
