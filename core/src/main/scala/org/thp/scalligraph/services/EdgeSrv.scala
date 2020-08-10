package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.structure.{Edge, Graph}
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}

import scala.util.{Failure, Success, Try}

class EdgeSrv[E <: Product, FROM <: Product, TO <: Product](
    implicit val db: Database,
    val model: Model.Edge[E]
) extends ElementSrv[E, Edge] {

  override def startTraversal(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    filterTraversal(Traversal.E())

  override def filterTraversal(
      traversal: Traversal[Edge, Edge, Converter.Identity[Edge]]
  ): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    db.labelFilter(model)(traversal).domainMap(model.converter)

  override def getByIds(ids: String*)(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    if (ids.isEmpty) Traversal.empty[Edge].domainMap(model.converter)
    else db.labelFilter(model)(Traversal.E(ids: _*)).domainMap(model.converter)

  def getOrFail(id: String)(implicit graph: Graph): Try[E with Entity] =
    get(id)
      .headOption
      .fold[Try[E with Entity]](Failure(NotFoundError(s"${model.label} $id not found")))(Success.apply)

  def get(edge: Edge)(implicit graph: Graph): Traversal[E with Entity, Edge, Converter[E with Entity, Edge]] =
    db.labelFilter(model)(Traversal.E(edge.id().toString)).domainMap(model.converter)

  def getOrFail(edge: Edge)(implicit graph: Graph): Try[E with Entity] =
    get(edge)
      .headOption
      .fold[Try[E with Entity]](Failure(NotFoundError(s"${model.label} ${edge.id()} not found")))(Success.apply)

  def create(e: E, from: FROM with Entity, to: TO with Entity)(implicit graph: Graph, authContext: AuthContext): Try[E with Entity] =
    Try(db.createEdge[E, FROM, TO](graph, authContext, model.asInstanceOf[Model.Edge[E]], e, from, to))
}
