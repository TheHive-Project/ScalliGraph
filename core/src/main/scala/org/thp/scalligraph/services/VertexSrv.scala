package org.thp.scalligraph.services

import java.util.Date

import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}
import org.thp.scalligraph.{EntityId, EntityIdOrName, NotFoundError}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, IdentityConverter, Traversal}
import play.api.libs.json.JsObject

import scala.util.{Failure, Success, Try}

abstract class VertexSrv[V <: Product](implicit db: Database, val model: Model.Vertex[V]) extends ElementSrv[V, Vertex] {
  override def startTraversal(implicit graph: Graph): Traversal.V[V] =
    filterTraversal(Traversal.V())

  override def filterTraversal(
      traversal: Traversal[Vertex, Vertex, Converter.Identity[Vertex]]
  ): Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] =
    db.labelFilter(model)(traversal).domainMap(model.converter)

  override def getByIds(ids: EntityId*)(implicit graph: Graph): Traversal.V[V] =
    if (ids.isEmpty) Traversal.empty[Vertex].domainMap(model.converter)
    else filterTraversal(Traversal.V(ids: _*))

  def get(vertex: Vertex)(implicit graph: Graph): Traversal.V[V] =
    filterTraversal(Traversal.V(EntityId(vertex.id())))

  def getOrFail(idOrName: EntityIdOrName)(implicit graph: Graph): Try[V with Entity] =
    get(idOrName)
      .headOption
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} $idOrName not found")))(Success.apply)

  def getOrFail(vertex: Vertex)(implicit graph: Graph): Try[V with Entity] =
    get(vertex)
      .headOption
      .fold[Try[V with Entity]](Failure(NotFoundError(s"${model.label} ${vertex.id()} not found")))(Success.apply)

  def createEntity(e: V)(implicit graph: Graph, authContext: AuthContext): Try[V with Entity] =
    Success(db.createVertex[V](graph, authContext, model, e))

  def exists(e: V)(implicit graph: Graph): Boolean = false

  def update(
      traversalSelect: Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]] => Traversal[
        V with Entity,
        Vertex,
        Converter[V with Entity, Vertex]
      ],
      propertyUpdaters: Seq[PropertyUpdater]
  )(implicit graph: Graph, authContext: AuthContext): Try[(Traversal.V[V], JsObject)] =
    update(traversalSelect(startTraversal), propertyUpdaters)

  def update(traversal: Traversal.V[V], propertyUpdaters: Seq[PropertyUpdater])(implicit
      graph: Graph,
      authContext: AuthContext
  ): Try[(Traversal[V with Entity, Vertex, Converter[V with Entity, Vertex]], JsObject)] = {
    val myClone = traversal.clone()
    logger.debug(s"Execution of ${traversal.raw} (update)")
    traversal
      .setConverter[Vertex, IdentityConverter[Vertex]](Converter.identity)
      .headOption
      .fold[Try[(Traversal.V[V], JsObject)]](Failure(NotFoundError(s"${model.label} not found"))) { vertex =>
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
