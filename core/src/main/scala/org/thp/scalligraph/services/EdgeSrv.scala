package org.thp.scalligraph.services

import gremlin.scala._
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

class EdgeSrv[E <: Product: ru.TypeTag, FROM <: Product: ru.TypeTag, TO <: Product: ru.TypeTag](implicit val db: Database)
    extends ElementSrv[E, Edge] {
  override val model: Model.Edge[E, FROM, TO] = db.getEdgeModel[E, FROM, TO]

  override def getByIds(ids: String*)(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    if (ids.isEmpty) new Traversal[E with Entity, Edge, Converter[E with Entity, Edge]](graph.inject(), model.converter)
    else new Traversal[E with Entity, Edge, Converter[E with Entity, Edge]](db.labelFilter(model)(graph.E(ids: _*)), model.converter)

  override def initSteps(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    new Traversal[E with Entity, Edge, Converter[E with Entity, Edge]](db.labelFilter(model)(graph.E), model.converter)

  def getOrFail(id: String)(implicit graph: Graph): Try[E with Entity] =
    get(id)
      .headOption()
      .fold[Try[E with Entity]](Failure(NotFoundError(s"${model.label} $id not found")))(Success.apply)

  def get(edge: Edge)(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    new Traversal[E with Entity, Edge, Converter[E with Entity, Edge]](db.labelFilter(model)(graph.E(edge)), model.converter)

  def getOrFail(edge: Edge)(implicit graph: Graph): Try[E with Entity] =
    get(edge)
      .headOption()
      .fold[Try[E with Entity]](Failure(NotFoundError(s"${model.label} ${edge.id()} not found")))(Success.apply)

  def create(e: E, from: FROM with Entity, to: TO with Entity)(implicit graph: Graph, authContext: AuthContext): Try[E with Entity] =
    Try(db.createEdge[E, FROM, TO](graph, authContext, model, e, from, to))
}
