package org.thp.scalligraph.services

import java.util.{Collection => JCollection, Map => JMap}

import gremlin.scala._
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.structure.T
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Database, Entity, IndexType, UniMapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps
import play.api.Logger
import shapeless.{::, HNil}

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

sealed trait GenIntegrityCheckOps
trait IntegrityCheckOps[E <: Product] extends GenIntegrityCheckOps {
  val db: Database
  val service: VertexSrv[E, _ <: VertexSteps[E]]

  lazy val name: String   = service.model.label
  lazy val logger: Logger = Logger(getClass)

  def getDuplicates[A](property: String): List[List[E with Entity]] = db.roTransaction { implicit graph =>
    service
      .initSteps
      .raw
      .groupCount(By(Key[A](property)))
      .unfold[JMap.Entry[A, Long]]()
      .where(_.selectValues.is(P.gt(1)))
      .selectKeys
      .toList
      .map { value =>
        service.initSteps.has(property, value).toList
      }
  }

  def copyEdge(from: E with Entity, to: E with Entity, predicate: Edge => Boolean = _ => true)(implicit graph: Graph): Unit = {
    val toVertex: Vertex = graph.V(to._id).head
    service.get(from).raw.outE().toList().filter(predicate).foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => Key(p.key()) -> p.value()).toList
      val label = edge.label()
      logger.debug(s"create edge from $toVertex to ${graph.E(edge.id()).inV().head()} with properties: $props")
      graph.E(edge.id()).inV().addE(label, props: _*).from(toVertex).iterate()
    }
    service.get(from).raw.inE().toList().filter(predicate).foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => Key(p.key()) -> p.value()).toList
      val label = edge.label()
      logger.debug(s"create edge from ${graph.E(edge.id()).outV().head()} to $toVertex with properties: $props")
      graph.E(edge.id()).outV().addE(label, props: _*).to(toVertex).iterate()
    }
  }

  def removeVertices(vertices: Seq[Vertex])(implicit graph: Graph): Unit =
    if (vertices.nonEmpty) {
      graph.V(vertices.map(_.id()).distinct: _*).drop().iterate()
      ()
    }

  def removeEdges(edges: Seq[Edge])(implicit graph: Graph): Unit =
    if (edges.nonEmpty) {
      graph.E(edges.map(_.id()).distinct: _*).drop().iterate()
      ()
    }

  def duplicateInEdges[EDGE <: Product: ru.TypeTag](from: GremlinScala[Vertex]): Seq[Seq[Edge]] = {
    val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
    duplicateLinks[Edge, Vertex](from, (_.inE(edgeName), _.inV()), (_.outV(), _.outE(edgeName)))
  }

  def duplicateOutEdges[EDGE <: Product: ru.TypeTag](from: GremlinScala[Vertex]): Seq[Seq[Edge]] = {
    val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
    duplicateLinks[Edge, Vertex](from, (_.outE(edgeName), _.outV()), (_.inV(), _.inE(edgeName)))
  }

  def duplicateLinks[EDGE <: Element, TO](
      from: GremlinScala[Vertex],
      fromEdge: (
          GremlinScala.Aux[Vertex, Vertex :: HNil] => GremlinScala.Aux[EDGE, Vertex :: HNil],
          GremlinScala.Aux[EDGE, HNil] => GremlinScala.Aux[Vertex, HNil]
      ),
      edgeTo: (
          GremlinScala.Aux[EDGE, Vertex :: EDGE :: HNil] => GremlinScala.Aux[TO, Vertex :: EDGE :: HNil],
          GremlinScala.Aux[TO, Vertex :: EDGE :: TO :: HNil] => GremlinScala.Aux[EDGE, Vertex :: EDGE :: TO :: HNil]
      )
  ): Seq[Seq[EDGE]] = {
    val fromLabel: StepLabel[Vertex] = StepLabel[Vertex]()
    val e1Label: StepLabel[EDGE]     = StepLabel[EDGE]()
    val toLabel: StepLabel[TO]       = StepLabel[TO]()
    ((_: Unit) => from.asInstanceOf[GremlinScala.Aux[Vertex, HNil]])
      .andThen(_.as(fromLabel))
      .andThen(fromEdge._1)
      .andThen(_.as(e1Label))
      .andThen(edgeTo._1)
      .andThen(_.as(toLabel))
      .andThen(edgeTo._2)
      .andThen(
        _.where(P.neq(e1Label.name))
          .where(fromEdge._2.andThen(_.as(fromLabel)))
          .group(By(__.select(fromLabel.name, e1Label.name, toLabel.name).by().by(T.label).by()))
          .unfold[JMap.Entry[Any, JCollection[EDGE]]]()
          .selectValues
          .where(_.count(Scope.local).is(P.gt(1)))
      )
      .apply(())
      .toList
      .map(_.asScala.groupBy(_.id()).map(_._2.head).toSeq)
  }

  def firstCreatedEntity(elements: Seq[E with Entity]): Option[(E with Entity, Seq[E with Entity])] =
    if (elements.isEmpty) None
    else {
      val firstIndex = elements.zipWithIndex.minBy(_._1._createdAt)._2
      Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
    }

  def lastCreatedEntity(elements: Seq[E with Entity]): Option[(E with Entity, Seq[E with Entity])] =
    if (elements.isEmpty) None
    else {
      val lastIndex = elements.zipWithIndex.maxBy(_._1._createdAt)._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  def firstUpdatedEntity(elements: Seq[E with Entity]): Option[(E with Entity, Seq[E with Entity])] =
    if (elements.isEmpty) None
    else {
      val firstIndex = elements.map(e => e._updatedAt.getOrElse(e._createdAt)).zipWithIndex.minBy(_._1)._2
      Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
    }

  def lastUpdatedEntity(elements: Seq[E with Entity]): Option[(E with Entity, Seq[E with Entity])] =
    if (elements.isEmpty) None
    else {
      val lastIndex = elements.map(e => e._updatedAt.getOrElse(e._createdAt)).zipWithIndex.maxBy(_._1)._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  def firstCreatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val firstIndex = elements.map(e => db.getSingleProperty(e, "_createdAt", UniMapping.date)).zipWithIndex.minBy(_._1)._2
      Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
    }

  def lastCreatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val lastIndex = elements.map(e => db.getSingleProperty(e, "_createdAt", UniMapping.date)).zipWithIndex.maxBy(_._1)._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  def firstUpdatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val firstIndex = elements
        .map(e =>
          db.getOptionProperty(e, "_updatedAt", UniMapping.date.optional)
            .getOrElse(db.getSingleProperty(e, "_createdAt", UniMapping.date))
        )
        .zipWithIndex
        .minBy(_._1)
        ._2
      Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
    }

  def lastUpdatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val lastIndex = elements
        .map(e =>
          db.getOptionProperty(e, "_updatedAt", UniMapping.date.optional)
            .getOrElse(db.getSingleProperty(e, "_createdAt", UniMapping.date))
        )
        .zipWithIndex
        .maxBy(_._1)
        ._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  lazy val uniqueProperties: Seq[String] = service.model.indexes.flatMap {
    case (IndexType.unique, properties) if properties.lengthCompare(1) == 0 => properties
    case (IndexType.unique, _) =>
      logger.warn("Index on only one property can be deduplicated")
      None
    case _ => Nil
  }

  def duplicateEntities: Seq[List[E with Entity]] = uniqueProperties.flatMap(getDuplicates)

  def check(): Unit =
    duplicateEntities
      .foreach { entities =>
        db.tryTransaction { implicit graph =>
          resolve(entities)
        }
      }

  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit =
    service.model.initialValues.filterNot(service.exists).foreach(service.createEntity)

  def resolve(entities: List[E with Entity])(implicit graph: Graph): Try[Unit]
}
