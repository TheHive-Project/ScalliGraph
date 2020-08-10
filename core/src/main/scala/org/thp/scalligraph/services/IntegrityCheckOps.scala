package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Database, Entity, IndexType, UMapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, IdentityConverter, StepLabel, Traversal}
import play.api.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

sealed trait GenIntegrityCheckOps {
  def check(): Unit
  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit
}

trait IntegrityCheckOps[E <: Product] extends GenIntegrityCheckOps {
  val db: Database
  val service: VertexSrv[E]

  lazy val name: String     = service.model.label
  lazy val logger: Logger   = Logger(getClass)
  final private val noValue = new Object

  def getDuplicates[A](properties: Seq[String]): Seq[Seq[E with Entity]] =
    if (properties.isEmpty) Nil
    else {
      val singleProperty = properties.lengthCompare(1) == 0
      val getValues: Vertex => Any =
        if (singleProperty) (_: Vertex).value[Any](properties.head)
        else (v: Vertex) => properties.map(v.property[Any](_).orElse(noValue))
      db.roTransaction { implicit graph =>
        val map = mutable.Map.empty[Any, mutable.Buffer[String]]
        service
          .startTraversal
          .setConverter[Vertex, IdentityConverter[Vertex]](Converter.identity)
          .toIterator
          .foreach { v =>
            map.getOrElseUpdate(getValues(v), mutable.Buffer.empty[String]) += v.id.toString
          }
        map
          .values
          .collect {
            case vertexIds if vertexIds.lengthCompare(1) > 0 => service.getByIds(vertexIds: _*).toList
          }
          .toSeq
      }
    }

  def copyEdge(from: E with Entity, to: E with Entity, predicate: Edge => Boolean = _ => true)(implicit graph: Graph): Unit = {
    val toVertex: Vertex = Traversal.V(to._id).head
    service.get(from).outE().toSeq.filter(predicate).foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => p.key() -> p.value())
      val label = edge.label()
      logger.debug(s"create edge from $toVertex to ${Traversal.E(edge.id().toString).inV.head} with properties: $props")
      val rawTraversal = Traversal
        .E(edge.id().toString)
        .inV
        .raw
      props
        .foldLeft(rawTraversal.addE(label).from(toVertex)) {
          case (edge, (key, value)) => edge.property(key, value)
        }
        .iterate()
    }
    service.get(from).inE().toSeq.filter(predicate).foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => p.key() -> p.value()).toSeq
      val label = edge.label()
      logger.debug(s"create edge from ${Traversal.E(edge.id().toString).outV.head} to $toVertex with properties: $props")
      val rawTraversal = Traversal
        .E(edge.id().toString)
        .outV
        .raw
      props
        .foldLeft(rawTraversal.addE(label).to(toVertex)) {
          case (edge, (key, value)) => edge.property(key, value)
        }
        .iterate()
    }
  }

  def removeVertices(vertices: Seq[Vertex])(implicit graph: Graph): Unit =
    if (vertices.nonEmpty) {
      Traversal.V(vertices.map(_.id().toString).distinct: _*).remove()
      ()
    }

  def removeEdges(edges: Seq[Edge])(implicit graph: Graph): Unit =
    if (edges.nonEmpty) {
      Traversal.E(edges.map(_.id().toString).distinct: _*).remove()
      ()
    }

  def duplicateInEdges[EDGE <: Product: ru.TypeTag](from: Traversal[_, Vertex, _]): Seq[Seq[Edge]] = {
    val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
    duplicateLinks[Edge, Vertex](from, (_.inE(edgeName), _.inV), (_.outV, _.outE(edgeName)))
  }

  def duplicateOutEdges[EDGE <: Product: ru.TypeTag](from: Traversal[_, Vertex, _]): Seq[Seq[Edge]] = {
    val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
    duplicateLinks[Edge, Vertex](from, (_.outE(edgeName), _.outV), (_.inV, _.inE(edgeName)))
  }

  def duplicateLinks[EDGE <: Element, TO](
      from: Traversal[_, Vertex, _],
      fromEdge: (
          Traversal.Identity[Vertex] => Traversal.Identity[EDGE],
          Traversal.Identity[EDGE] => Traversal.Identity[Vertex]
      ),
      edgeTo: (
          Traversal.Identity[EDGE] => Traversal.Identity[TO],
          Traversal.Identity[TO] => Traversal.Identity[EDGE]
      )
  ): Seq[Seq[EDGE]] = {
    val fromLabel = StepLabel.identity[Vertex]
    val e1Label   = StepLabel.identity[EDGE]
    val toLabel   = StepLabel.identity[TO]
    ((_: Unit) => from.setConverter[Vertex, Converter.Identity[Vertex]](Converter.identity[Vertex]))
      .andThen(_.as(fromLabel))
      .andThen(fromEdge._1)
      .andThen(_.as(e1Label))
      .andThen(edgeTo._1)
      .andThen(_.as(toLabel))
      .andThen(edgeTo._2)
      .andThen(
        _.where(P.neq(e1Label.name))
          .where(fromEdge._2.andThen(_.as(fromLabel)))
          .group(_.by(_.select(_(fromLabel)(_.by).apply(e1Label)(_.byLabel).apply(toLabel)(_.by)))) //, toLabel)).by().by(T.label).by()))
          .unfold
          .selectValues
          .where(_.localCount.is(P.gt(1)))
      )
      .apply(())
      .toSeq
      .map(_.groupBy(_.id()).map(_._2.head).toSeq)
  }

  /*
  Error:(135, 68) type mismatch;
 found   : org.thp.scalligraph.steps.GenericBySelector[EDGE,EDGE,org.thp.scalligraph.steps.Converter.Identity[EDGE]] => org.thp.scalligraph.steps.ByResult[EDGE,Nothing,Nothing,Nothing]
    (which expands to)  org.thp.scalligraph.steps.GenericBySelector[EDGE,EDGE,org.thp.scalligraph.steps.IdentityConverter[EDGE]] => org.thp.scalligraph.steps.ByResult[EDGE,Nothing,Nothing,Nothing]
    (which expands to)  org.thp.scalligraph.steps.GenericBySelector[EDGE,EDGE,org.thp.scalligraph.steps.IdentityConverter[EDGE]] => org.thp.scalligraph.steps.ByResult[EDGE,DD,Nothing,Nothing]
 required: org.thp.scalligraph.steps.GenericBySelector[EDGE,EDGE,org.thp.scalligraph.steps.Converter.Identity[EDGE]] => org.thp.scalligraph.steps.ByResult[EDGE,DD,Nothing,Nothing]
          .group(_.by(_.select(_(fromLabel)(_.by).apply(e1Label)(_.byLabel).apply(toLabel)(_.by)))) //, toLabel)).by().by(T.label).by()))
   */
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
      val firstIndex = elements.map(e => UMapping.date.getProperty(e, "_createdAt")).zipWithIndex.minBy(_._1)._2
      Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
    }

  def lastCreatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val lastIndex = elements.map(e => UMapping.date.getProperty(e, "_createdAt")).zipWithIndex.maxBy(_._1)._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  def firstUpdatedElement[ELEMENT <: Element](elements: Seq[ELEMENT]): Option[(ELEMENT, Seq[ELEMENT])] =
    if (elements.isEmpty) None
    else {
      val firstIndex = elements
        .map(e =>
          UMapping
            .date
            .optional
            .getProperty(e, "_updatedAt")
            .getOrElse(UMapping.date.getProperty(e, "_createdAt"))
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
          UMapping
            .date
            .optional
            .getProperty(e, "_updatedAt")
            .getOrElse(UMapping.date.getProperty(e, "_createdAt"))
        )
        .zipWithIndex
        .maxBy(_._1)
        ._2
      Some((elements(lastIndex), elements.patch(lastIndex, Nil, 1)))
    }

  lazy val uniqueProperties: Option[Seq[String]] = service.model.indexes.collectFirst {
    case (IndexType.unique, properties) => properties
  }

  def duplicateEntities: Seq[Seq[E with Entity]] = uniqueProperties.fold[Seq[Seq[E with Entity]]](Nil)(getDuplicates)

  def check(): Unit =
    duplicateEntities
      .foreach { entities =>
        db.tryTransaction { implicit graph =>
          logger.info(s"Found duplicate entities:${entities.map(e => s"\n - $e").mkString}")
          resolve(entities)
        }
      }

  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit =
    service.model.initialValues.filterNot(service.exists).foreach(service.createEntity)

  def resolve(entities: Seq[E with Entity])(implicit graph: Graph): Try[Unit]
}
