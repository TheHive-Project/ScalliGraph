package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Vertex}
import org.thp.scalligraph.EntityId
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Database, Entity, IndexType, UMapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal._
import org.thp.scalligraph.utils.FunctionalCondition.When
import play.api.Logger

import java.util.{Collection => JCollection, List => JList}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

sealed trait GenIntegrityCheckOps {
  def name: String
  def duplicationCheck(): Map[String, Int]
  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit
}

trait EntitySelector[E]             extends (Seq[E with Entity] => Option[(E with Entity, Seq[E with Entity])])
trait ElementSelector[E <: Element] extends (Seq[E] => Option[(E, Seq[E])])

object ElementSelector {
  def firstCreatedElement[ELEMENT <: Element]: ElementSelector[ELEMENT] =
    (elements: Seq[ELEMENT]) =>
      if (elements.isEmpty) None
      else {
        val firstIndex = elements.map(e => UMapping.date.getProperty(e, "_createdAt")).zipWithIndex.minBy(_._1)._2
        Some((elements(firstIndex), elements.patch(firstIndex, Nil, 1)))
      }
}

object EntitySelector {
  def firstCreatedEntity[E]: EntitySelector[E] =
    (entities: Seq[E with Entity]) =>
      if (entities.isEmpty) None
      else {
        val selected = entities.minBy(_._createdAt)
        val (a, b)   = entities.span(_ != selected)
        Some((selected, a ++ b.tail))
      }
  def lastCreatedEntity[E]: EntitySelector[E] =
    (entities: Seq[E with Entity]) =>
      if (entities.isEmpty) None
      else {
        val selected = entities.maxBy(_._createdAt)
        val (a, b)   = entities.span(_ != selected)
        Some((selected, a ++ b.tail))
      }
}

trait LinkRemover extends ((Entity, Entity) => Unit)

trait OrphanStrategy[E <: Product, I] extends ((I, E with Entity) => Map[String, Int])

trait IntegrityCheckOps[E <: Product] extends GenIntegrityCheckOps {
  val db: Database
  val service: VertexSrv[E]

  lazy val name: String     = service.model.label
  lazy val logger: Logger   = Logger(getClass)
  final private val noValue = new Object

  implicit class MapMerger(m1: Map[String, Int]) {
    def <+>(m2: Map[String, Int]): Map[String, Int] = (m1.keySet ++ m2.keySet).map(k => k -> (m1.getOrElse(k, 0) + m2.getOrElse(k, 0))).toMap
  }

  class LinkRemoverSelector {
    def outEdge[EDGE <: Product: ru.TypeTag](implicit graph: Graph): LinkRemover = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      (from, to) => service.getByIds(from._id).outE(edgeName).filter(_.inV.hasId(to._id)).remove()
    }
  }

  class SingleLinkChecker[L <: Product: ru.TypeTag, EI, LI](
      orphanStrategy: OrphanStrategy[E, EI],
      setField: (E with Entity, L with Entity) => Unit,
      entitySelector: (E with Entity) => EntitySelector[L],
      removeLink: LinkRemover,
      getLink: LI => L with Entity,
      optionalField: EI => Option[LI]
  ) {
    lazy val linkName: String = ru.typeOf[L].typeSymbol.name.toString

    def check(entity: E with Entity, field: EI, links: Seq[LI]): Map[String, Int] = {
      val of = optionalField(field)
      // [field:A] ---> [_id:A]
      if (of.toSeq == links) Map.empty

      // [field:A] ---> [_id:A]
      //           \--> [_id:B]
      else if (of.fold(false)(l => links.contains(l))) {
        val count = links
          .filterNot(_ == of.get)
          .map(i => removeLink(entity, getLink(i)))
          .size
        Map(s"$name-$linkName-unlink" -> count)
      }

      // [field:?] ---> [_id:A] or no link
      //           \--> [_id:B]
      else
        entitySelector(entity)(links.map(getLink)).fold(orphanStrategy(field, entity)) {
          case (selectedLink, extraLinks) =>
            setField(entity, selectedLink)
            val count = extraLinks.map(l => removeLink(entity, l)).size

            Map(s"$name-$linkName-setField" -> 1)
              .when(count > 0)(_ + (s"$name-$linkName-unlink" -> count))
        }
    }
  }

  class MultiLinkChecker[L <: Product: ru.TypeTag, I](
      orphanStrategy: OrphanStrategy[E, Seq[I]],
      setField: (E with Entity, Seq[L with Entity]) => Unit,
      getLink: I => L with Entity
  ) {
    lazy val linkName: String = ru.typeOf[L].typeSymbol.name.toString

    def check(entity: E with Entity, fields: Seq[I], links: Seq[I]): Map[String, Int] =
      // [field:ABC] ---> [links:ABC]
      if (fields == links) Map.empty

      // [field:ABC] --->
      else if (links.isEmpty) orphanStrategy(fields, entity)

      // [field:ABC] ---> [links:(B)CD]
      else {
        val extraLinks  = (links.toSet -- fields).size
        val extraFields = (fields.toSet -- links).size
        setField(entity, links.map(getLink))

        Map
          .empty[String, Int]
          .when(extraLinks > 0)(_ + (s"$name-$linkName-extraLinks" -> extraLinks))
          .when(extraFields > 0)(_ + (s"$name-$linkName-extraLinks" -> extraLinks))
      }
  }

  class OrphanStrategySelector[I](fieldName: String)(implicit graph: Graph) {
    def remove: OrphanStrategy[E, I] = {
      case (_, entity) =>
        service.get(entity).remove()
        Map(s"$name-$fieldName-removeOrphan" -> 1)
    }

    def set(emptyValue: I)(implicit mapping: UMapping[I]): OrphanStrategy[E, I] = {
      case (fieldValue, _) if fieldValue == emptyValue => Map.empty
      case (_, entity) =>
        mapping.toMapping.setProperty(service.get(entity), fieldName, emptyValue).iterate()
        Map(s"$name-$fieldName-removeOrphan" -> 1)
    }
  }

  def singleIdLink[L <: Product: ru.TypeTag](
      fieldName: String,
      linkService: VertexSrv[L]
  )(
      linkRemover: LinkRemoverSelector => LinkRemover,
      orphanStrategy: OrphanStrategySelector[EntityId] => OrphanStrategy[E, EntityId],
      entitySelector: E with Entity => EntitySelector[L] = (_: E with Entity) => EntitySelector.firstCreatedEntity[L]
  )(implicit graph: Graph) =
    new SingleLinkChecker[L, EntityId, EntityId](
      orphanStrategy(new OrphanStrategySelector[EntityId](fieldName)),
      (entity, link) => UMapping.entityId.setProperty(service.get(entity), fieldName, link._id).iterate(),
      entitySelector,
      linkRemover(new LinkRemoverSelector),
      linkService.getOrFail(_).get,
      Some(_)
    )

  def singleLink[L <: Product: ru.TypeTag, I](fieldName: String, getLink: I => L with Entity, linkValue: L with Entity => I)(
      linkRemover: LinkRemoverSelector => LinkRemover,
      orphanStrategy: OrphanStrategySelector[I] => OrphanStrategy[E, I] = (_: OrphanStrategySelector[I]).remove,
      entitySelector: E with Entity => EntitySelector[L] = (_: E with Entity) => EntitySelector.firstCreatedEntity[L]
  )(implicit graph: Graph, mapping: UMapping[I]) =
    new SingleLinkChecker[L, I, I](
      orphanStrategy(new OrphanStrategySelector[I](fieldName)),
      (entity, link) => mapping.toMapping.setProperty(service.get(entity), fieldName, linkValue(link)).iterate(),
      entitySelector,
      linkRemover(new LinkRemoverSelector),
      getLink,
      Some(_)
    )

  def singleOptionLink[L <: Product: ru.TypeTag, I](
      fieldName: String,
      getLink: I => L with Entity,
      linkValue: L with Entity => I
  )(
      linkRemover: LinkRemoverSelector => LinkRemover,
      orphanStrategy: OrphanStrategySelector[Option[I]] => OrphanStrategy[E, Option[I]] = (_: OrphanStrategySelector[Option[I]]).remove,
      entitySelector: E with Entity => EntitySelector[L] = (_: E with Entity) => EntitySelector.firstCreatedEntity[L]
  )(implicit graph: Graph, mapping: UMapping[Option[I]]) =
    new SingleLinkChecker[L, Option[I], I](
      orphanStrategy(new OrphanStrategySelector[Option[I]](fieldName)),
      (entity, link) => mapping.toMapping.setProperty(service.get(entity), fieldName, Some(linkValue(link))).iterate(),
      entitySelector,
      linkRemover(new LinkRemoverSelector),
      getLink,
      identity
    )

  def multiLink[L <: Product: ru.TypeTag, I](fieldName: String, getLink: I => L with Entity, linkValue: L with Entity => I)(
      orphanStrategy: OrphanStrategySelector[Seq[I]] => OrphanStrategy[E, Seq[I]]
  )(implicit graph: Graph, mapping: UMapping[Seq[I]]) =
    new MultiLinkChecker[L, I](
      orphanStrategy(new OrphanStrategySelector[Seq[I]](fieldName)),
      (entity, links) => mapping.toMapping.setProperty(service.get(entity), fieldName, links.map(linkValue)).iterate(),
      getLink
    )

  def multiIdLink[L <: Product: ru.TypeTag](fieldName: String, linkService: VertexSrv[L])(
      orphanStrategy: OrphanStrategySelector[Seq[EntityId]] => OrphanStrategy[E, Seq[EntityId]]
  )(implicit graph: Graph) =
    new MultiLinkChecker[L, EntityId](
      orphanStrategy(new OrphanStrategySelector[Seq[EntityId]](fieldName)),
      (entity, links) => UMapping.entityId.sequence.setProperty(service.get(entity), fieldName, links.map(_._id)).iterate(),
      linkService.getOrFail(_).get
    )

  implicit class IntegrityDSL(traversal: Traversal.V[E]) {
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
    ): Traversal[Seq[Seq[EDGE]], JList[JCollection[EDGE]], Converter[Seq[Seq[EDGE]], JList[JCollection[EDGE]]]] /*Seq[Seq[EDGE]]*/ = {
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
        .fold
        .domainMap(_.map(_.groupBy(_.id()).map(_._2.head).toSeq))
    }

    private def doRemoveEdge(name: String, entitySelector: ElementSelector[Edge])(edges: Seq[Seq[Edge]]): Map[String, Int] = {
      val count = edges.flatMap(entitySelector(_)).flatMap(_._2).map(_.remove()).size
      if (count > 0) Map(name -> count)
      else Map.empty
    }

    def removeDuplicateInEdges[EDGE <: Product: ru.TypeTag](
        entitySelector: ElementSelector[Edge] = ElementSelector.firstCreatedElement[Edge]
    ): Traversal[Map[String, Int], JList[JCollection[Edge]], Converter[Map[String, Int], JList[JCollection[Edge]]]] = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      duplicateLinks[Edge, Vertex](traversal, (_.inE(edgeName), _.inV), (_.outV, _.outE(edgeName)))
        .domainMap(doRemoveEdge(s"$name-$edgeName-extraEdge", entitySelector))
    }

    def removeDuplicateOutEdges[EDGE <: Product: ru.TypeTag](
        entitySelector: ElementSelector[Edge] = ElementSelector.firstCreatedElement[Edge]
    ): Traversal[Map[String, Int], JList[JCollection[Edge]], Converter[Map[String, Int], JList[JCollection[Edge]]]] = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      duplicateLinks[Edge, Vertex](traversal, (_.outE(edgeName), _.outV), (_.inV, _.inE(edgeName)))
        .domainMap(doRemoveEdge(s"$name-$edgeName-extraEdge", entitySelector))
    }

  }

  def getDuplicates[A](properties: Seq[String]): Seq[Seq[E with Entity]] =
    if (properties.isEmpty) Nil
    else {
      val singleProperty = properties.lengthCompare(1) == 0
      val getValues: Vertex => Any =
        if (singleProperty) (_: Vertex).value[Any](properties.head)
        else (v: Vertex) => properties.map(v.property[Any](_).orElse(noValue))
      db.roTransaction { implicit graph =>
        val map = mutable.Map.empty[Any, mutable.Buffer[EntityId]]
        service
          .startTraversal
          .setConverter[Vertex, IdentityConverter[Vertex]](Converter.identity)
          .foreach { v =>
            map.getOrElseUpdate(getValues(v), mutable.Buffer.empty[EntityId]) += EntityId(v.id)
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
    val toVertex: Vertex = graph.V(to._label, to._id).head
    service.get(from).outE().toSeq.filter(predicate).foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => p.key() -> p.value())
      val label = edge.label()
      logger.debug(s"create edge from $toVertex to ${graph.E(edge.label(), EntityId(edge.id())).inV.head} with properties: $props")
      val rawTraversal = graph
        .E(edge.label(), EntityId(edge.id()))
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
      logger.debug(s"create edge from ${graph.E(edge.label(), EntityId(edge.id())).outV.head} to $toVertex with properties: $props")
      val rawTraversal = graph
        .E(edge.label(), EntityId(edge.id()))
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
      graph.V(vertices.head.label(), vertices.map(v => EntityId(v.id())).distinct: _*).remove()
      ()
    }

  def removeEdges(edges: Seq[Edge])(implicit graph: Graph): Unit =
    if (edges.nonEmpty) {
      graph.E(edges.head.label(), edges.map(e => EntityId(e.id())).distinct: _*).remove()
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

  lazy val uniqueProperties: Option[Seq[String]] = service.model.indexes.collectFirst {
    case (IndexType.unique, properties) => properties
  }

  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit =
    service.model.initialValues.filterNot(service.exists).foreach(service.createEntity)

  def findDuplicates(): Seq[Seq[E with Entity]] = uniqueProperties.fold[Seq[Seq[E with Entity]]](Nil)(getDuplicates)

  def duplicationCheck(): Map[String, Int] = {
    val duplicates = findDuplicates()
    duplicates
      .foreach { entities =>
        db.tryTransaction { implicit graph =>
          logger.info(s"Found duplicate entities:${entities.map(e => s"\n - $e").mkString}")
          resolve(entities)
        }
      }
    Map("duplicate" -> duplicates.length)
  }

  def resolve(entities: Seq[E with Entity])(implicit graph: Graph): Try[Unit]

  def globalCheck(): Map[String, Int]
}
