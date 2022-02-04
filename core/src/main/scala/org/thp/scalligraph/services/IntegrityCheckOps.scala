package org.thp.scalligraph.services

import bloomfilter.CanGenerateHashFrom
import bloomfilter.CanGenerateHashFrom.CanGenerateHashFromLong
import bloomfilter.mutable.BloomFilter
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Vertex}
import org.thp.scalligraph.{EntityId, RichFiniteDuration}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Database, Entity, IndexType, UMapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal._
import org.thp.scalligraph.utils.FunctionalCondition.When
import play.api.Logger

import java.util.{Date, Collection => JCollection, List => JList}
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.duration._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

case class CheckPerformance(period: Option[Long], duration: Option[Long])

trait EntitySelector[E]             extends (Seq[E with Entity] => Option[(E with Entity, Seq[E with Entity])])
trait ElementSelector[E <: Element] extends (Seq[E] => Option[(E, Seq[E])])

object ElementSelector {
  private def generic[ELEMENT <: Element](select: Seq[ELEMENT] => ELEMENT): ElementSelector[ELEMENT] = { (elements: Seq[ELEMENT]) =>
    if (elements.isEmpty) None
    else {
      val selected = select(elements)
      Some((selected, elements.filterNot(_.id() == selected.id())))
    }
  }
  def firstCreatedElement[ELEMENT <: Element]: ElementSelector[ELEMENT] = generic(_.minBy(e => UMapping.date.getProperty(e, "_createdAt")))
  def lastCreatedElement[ELEMENT <: Element]: ElementSelector[ELEMENT]  = generic(_.maxBy(e => UMapping.date.getProperty(e, "_createdAt")))
  def firstUpdatedElement[ELEMENT <: Element]: ElementSelector[ELEMENT] = generic(_.minBy(e => UMapping.date.getProperty(e, "_updatedAt")))
  def lastUpdatedElement[ELEMENT <: Element]: ElementSelector[ELEMENT]  = generic(_.maxBy(e => UMapping.date.getProperty(e, "_updatedAt")))
}

object EntitySelector {
  private def generic[E](select: Seq[E with Entity] => E with Entity): EntitySelector[E] =
    (entities: Seq[E with Entity]) =>
      if (entities.isEmpty) None
      else {
        val selected = select(entities)
        Some((selected, entities.filterNot(_._id == selected._id)))
      }
  def lastCreatedEntity[E]: EntitySelector[E]  = generic(_.maxBy(_._createdAt))
  def firstCreatedEntity[E]: EntitySelector[E] = generic(_.minBy(_._createdAt))
  def lastUpdatedEntity[E]: EntitySelector[E]  = generic(_.maxBy(_._updatedAt))
  def firstUpdatedEntity[E]: EntitySelector[E] = generic(_.minBy(_._updatedAt))
}

trait LinkRemover extends ((Entity, Entity) => Unit)

trait OrphanStrategy[E <: Product, I] extends ((I, E with Entity) => Map[String, Long])

trait MapMerger {
  implicit class MapMergerDefs(m1: Map[String, Long]) {
    def <+>(m2: Map[String, Long]): Map[String, Long] = (m1.keySet ++ m2.keySet).map(k => k -> (m1.getOrElse(k, 0L) + m2.getOrElse(k, 0L))).toMap
  }
}
object MapMerger extends MapMerger

trait KillSwitch {
  def reset(): Unit
  def continueProcess: Boolean
}
object KillSwitch {
  val alwaysOn: KillSwitch = new KillSwitch {
    override def continueProcess: Boolean = true
    override def reset(): Unit            = ()
  }
}

trait IntegrityCheck {
  type ENTITY <: Product
  lazy val name: String = service.model.label
  val db: Database
  val service: VertexSrv[ENTITY]
  def initialCheck()(implicit graph: Graph, authContext: AuthContext): Unit = ()
}

trait IntegrityCheckOps[E <: Product] extends IntegrityCheck with MapMerger {
  override type ENTITY = E
  val service: VertexSrv[ENTITY]

  lazy val logger: Logger = Logger(getClass)

  class LinkRemoverSelector {
    def outEdge[EDGE <: Product: ru.TypeTag](implicit graph: Graph): LinkRemover = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      (from, to) => service.getByIds(from._id).outE(edgeName).filter(_.inV.hasId(to._id)).remove()
    }

    def inEdge[EDGE <: Product: ru.TypeTag](implicit graph: Graph): LinkRemover = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      (from, to) => service.getByIds(from._id).inE(edgeName).filter(_.outV.hasId(to._id)).remove()
    }
  }

  class SingleLinkChecker[L <: Product: ru.TypeTag, EI, LI](
      orphanStrategy: OrphanStrategy[ENTITY, EI],
      setField: (ENTITY with Entity, L with Entity) => Unit,
      entitySelector: (ENTITY with Entity) => EntitySelector[L],
      removeLink: LinkRemover,
      getLink: LI => L with Entity,
      optionalField: EI => Option[LI]
  ) {
    lazy val linkName: String = ru.typeOf[L].typeSymbol.name.toString

    def check(entity: ENTITY with Entity, field: EI, links: Seq[LI]): Map[String, Long] = {
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
          .toLong
        Map(s"$name-$linkName-unlink" -> count)
      }

      // [field:?] ---> [_id:A] or no link
      //           \--> [_id:B]
      else
        entitySelector(entity)(links.map(getLink)).fold(orphanStrategy(field, entity)) {
          case (selectedLink, extraLinks) =>
            setField(entity, selectedLink)
            val count = extraLinks.map(l => removeLink(entity, l)).size.toLong

            Map(s"$name-$linkName-setField" -> 1L)
              .when(count > 0)(_ + (s"$name-$linkName-unlink" -> count))
        }
    }
  }

  class MultiLinkChecker[L <: Product: ru.TypeTag, I](
      orphanStrategy: OrphanStrategy[ENTITY, Set[I]],
      setField: (ENTITY with Entity, Seq[L with Entity]) => Unit,
      getLink: I => L with Entity
  ) {
    lazy val linkName: String = ru.typeOf[L].typeSymbol.name.toString

    def check(entity: ENTITY with Entity, fields: Set[I], links: Seq[I]): Map[String, Long] =
      // [field:ABC] ---> [links:ABC]
      if (fields == links.toSet) Map.empty

      // [field:ABC] --->
      else if (links.isEmpty) orphanStrategy(fields, entity)

      // [field:ABC] ---> [links:(B)CD]
      else {
        val extraLinks  = (links.toSet -- fields).size.toLong
        val extraFields = (fields -- links).size.toLong
        setField(entity, links.map(getLink))

        Map
          .empty[String, Long]
          .when(extraLinks > 0)(_ + (s"$name-$linkName-extraLinks" -> extraLinks))
          .when(extraFields > 0)(_ + (s"$name-$linkName-extraLinks" -> extraLinks))
      }
  }

  class OrphanStrategySelector[I](fieldName: String)(implicit graph: Graph, mapping: UMapping[I]) {
    def remove: OrphanStrategy[ENTITY, I] = {
      case (_, entity) =>
        service.get(entity).remove()
        Map(s"$name-$fieldName-removeOrphan" -> 1)
    }

    def set(emptyValue: I): OrphanStrategy[ENTITY, I] = {
      case (fieldValue, _) if fieldValue == emptyValue => Map.empty
      case (_, entity) =>
        mapping.toMapping.setProperty(service.get(entity), fieldName, emptyValue).iterate()
        Map(s"$name-$fieldName-setEmptyOrphan" -> 1)
    }
  }

  def singleIdLink[L <: Product: ru.TypeTag](
      fieldName: String,
      linkService: VertexSrv[L]
  )(
      linkRemover: LinkRemoverSelector => LinkRemover,
      orphanStrategy: OrphanStrategySelector[EntityId] => OrphanStrategy[ENTITY, EntityId],
      entitySelector: ENTITY with Entity => EntitySelector[L] = (_: ENTITY with Entity) => EntitySelector.firstCreatedEntity[L]
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
      orphanStrategy: OrphanStrategySelector[I] => OrphanStrategy[ENTITY, I] = (_: OrphanStrategySelector[I]).remove,
      entitySelector: ENTITY with Entity => EntitySelector[L] = (_: ENTITY with Entity) => EntitySelector.firstCreatedEntity[L]
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
      orphanStrategy: OrphanStrategySelector[Option[I]] => OrphanStrategy[ENTITY, Option[I]] = (_: OrphanStrategySelector[Option[I]]).set(None),
      entitySelector: ENTITY with Entity => EntitySelector[L] = (_: ENTITY with Entity) => EntitySelector.firstCreatedEntity[L]
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
      orphanStrategy: OrphanStrategySelector[Set[I]] => OrphanStrategy[ENTITY, Set[I]]
  )(implicit graph: Graph, mapping: UMapping[Set[I]]) =
    new MultiLinkChecker[L, I](
      orphanStrategy(new OrphanStrategySelector[Set[I]](fieldName)),
      (entity, links) => mapping.toMapping.setProperty(service.get(entity), fieldName, links.map(linkValue).toSet).iterate(),
      getLink
    )

  def multiIdLink[L <: Product: ru.TypeTag](fieldName: String, linkService: VertexSrv[L])(
      orphanStrategy: OrphanStrategySelector[Set[EntityId]] => OrphanStrategy[ENTITY, Set[EntityId]]
  )(implicit graph: Graph) =
    new MultiLinkChecker[L, EntityId](
      orphanStrategy(new OrphanStrategySelector[Set[EntityId]](fieldName)),
      (entity, links) => UMapping.entityId.set.setProperty(service.get(entity), fieldName, links.map(_._id).toSet).iterate(),
      linkService.getOrFail(_).get
    )

  implicit class IntegrityDSL(traversal: Traversal.V[ENTITY]) {
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

    private def doRemoveEdge(name: String, entitySelector: ElementSelector[Edge])(edges: Seq[Seq[Edge]]): Map[String, Long] = {
      val count = edges.flatMap(entitySelector(_)).flatMap(_._2).map(_.remove()).size.toLong
      if (count > 0) Map(name -> count)
      else Map.empty
    }

    def removeDuplicateInEdges[EDGE <: Product: ru.TypeTag](
        entitySelector: ElementSelector[Edge] = ElementSelector.firstCreatedElement[Edge]
    ): Traversal[Map[String, Long], JList[JCollection[Edge]], Converter[Map[String, Long], JList[JCollection[Edge]]]] = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      duplicateLinks[Edge, Vertex](traversal, (_.inE(edgeName), _.inV), (_.outV, _.outE(edgeName)))
        .domainMap(doRemoveEdge(s"$name-$edgeName-extraEdge", entitySelector))
    }

    def removeDuplicateOutEdges[EDGE <: Product: ru.TypeTag](
        entitySelector: ElementSelector[Edge] = ElementSelector.firstCreatedElement[Edge]
    ): Traversal[Map[String, Long], JList[JCollection[Edge]], Converter[Map[String, Long], JList[JCollection[Edge]]]] = {
      val edgeName = ru.typeOf[EDGE].typeSymbol.name.toString
      duplicateLinks[Edge, Vertex](traversal, (_.outE(edgeName), _.outV), (_.inV, _.inE(edgeName)))
        .domainMap(doRemoveEdge(s"$name-$edgeName-extraEdge", entitySelector))
    }

  }

  def copyEdge(from: ENTITY with Entity, to: ENTITY with Entity, predicate: Edge => Boolean = _ => true)(implicit graph: Graph): Unit = {
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

}

trait DedupCheck[E <: Product] extends IntegrityCheck with IntegrityCheckOps[E] {
  override type ENTITY = E

  lazy val uniqueProperties: Option[Seq[String]] = service.model.indexes.collectFirst {
    case (IndexType.unique, properties) => properties
  }

  def getDuplicates[A](properties: Seq[String], killSwitch: KillSwitch): Seq[Seq[E with Entity]] =
    if (properties.isEmpty) Nil
    else {
      implicit val CanGenerateHashFromVertex: CanGenerateHashFrom[Vertex] = (vertex: Vertex) =>
        properties.foldLeft(0L) { (h, p) =>
          vertex.property[Any](p).orElse(NO_VALUE) match {
            case s: String => CanGenerateHashFrom.canGenerateHashFromString.generateHash(s) ^ h
            case l: Long   => CanGenerateHashFromLong.generateHash(l) ^ h
            case other     => other.##.toLong ^ h
          }
        }

      val numberOfElements =
        try db.roTransaction(g => g.indexCountQuery(s"""v."_label":$name"""))
        catch {
          case error: Throwable =>
            logger.error("Index fetch error", error)
            1000000L
        }

      val bloomFilter = BloomFilter[Vertex](math.max(numberOfElements, 1L), 0.1)
      try db.roTransaction { implicit graph =>
        service
          .startTraversal
          .unsetConverter
          .toIterator
          .flatMap { vertex =>
            if (bloomFilter.mightContain(vertex)) {
              val entities = properties
                .foldLeft(service.startTraversal) { (t, p) =>
                  if (vertex.property[Any](p).isPresent)
                    t.unsafeHas(p, vertex.value[Any](p))
                  else
                    t.unsafeHasNot(p)
                }
                .toSeq
              if (entities.lengthCompare(1) > 0) Seq(entities)
              else Nil
            } else {
              bloomFilter.add(vertex)
              Nil
            }
          }
          .takeWhile(_ => killSwitch.continueProcess)
          .foldLeft((List.empty[Seq[E with Entity]], Set.empty[Set[EntityId]])) {
            case ((uniqueEntities, entityIds), es) =>
              val ids = es.map(_._id).toSet
              if (entityIds.contains(ids))
                (uniqueEntities, entityIds)
              else
                (es :: uniqueEntities, entityIds + ids)
          }
          ._1
      //          .distinctBy(_.map(_._id).toSet)
      //          .toList
      } finally bloomFilter.dispose()
    }

  def findDuplicates(killSwitch: KillSwitch): Seq[Seq[E with Entity]] =
    uniqueProperties.fold[Seq[Seq[E with Entity]]](Nil)(getDuplicates(_, killSwitch))

  def dedup(killSwitch: KillSwitch): Map[String, Long] = {
    val duplicates = findDuplicates(killSwitch)
    duplicates
      .toIterator
      .takeWhile(_ => killSwitch.continueProcess)
      .foreach { entities =>
        db.tryTransaction { implicit graph =>
          logger.debug(s"Found duplicate entities:${entities.map(e => s"\n - $e").mkString}")
          resolve(entities)
        }
      }
    Map("duplicate" -> duplicates.length.toLong)
  }

  def entitySelector: EntitySelector[E] = EntitySelector.firstCreatedEntity
  def resolve(entities: Seq[E with Entity])(implicit graph: Graph): Try[Unit] = {
    entitySelector(entities).foreach {
      case (head, tail) =>
        tail.foreach(copyEdge(_, head))
        service.getByIds(tail.map(_._id): _*).remove()
    }
    Success(())
  }
}

trait GlobalCheck[E <: Product] extends IntegrityCheck with MapMerger {
  override type ENTITY = E
  def globalCheck(traversal: Traversal.V[E])(implicit graph: Graph): Map[String, Long]

  def extraFilter(traversal: Traversal.V[E]): Traversal.V[E] = traversal

  final def getPerformanceIndicator: CheckPerformance =
    db.roTransaction { graph =>
      val duration = Try(graph.variables.get[Long](s"integrityCheckState-$name-duration").get()).toOption
      val period   = Try(graph.variables.get[Long](s"integrityCheckState-$name-period").get()).toOption
      CheckPerformance(period, duration)
    }

  final def runGlobalCheck(maxDuration: FiniteDuration, killSwitch: KillSwitch): Map[String, Long] = {
    logger.info(s"Starting $name integrity check for ${maxDuration.prettyPrint}")
    val startAt = System.currentTimeMillis()
    val stopAt  = startAt + maxDuration.toMillis
    val createdAtCursor = Try(
      db.roTransaction(_.variables.get[Date](s"integrityCheckState-$name-createdAtCursor").asScala)
    ).toOption.flatten
    val result = service
      .pagedTraversalIds(db, 100, t => extraFilter(createdAtCursor.fold(t)(c => t.unsafeHas("_createdAt", P.lt(c))))) { ids =>
        if (System.currentTimeMillis() > stopAt || !killSwitch.continueProcess) {
          logger.info(s"Check is stopped before the end (${if (killSwitch.continueProcess) "timeout" else "cancelled by user"})")
          db.tryTransaction { implicit graph =>
            Try {
              val newCursor = service.getByIds(ids.head).property("_createdAt", UMapping.date).head
              val period    = createdAtCursor.fold(System.currentTimeMillis())(_.getTime) - newCursor.getTime
              graph.variables.set(s"integrityCheckState-$name-duration", math.max(stopAt - startAt, 1L))
              graph.variables.set(s"integrityCheckState-$name-createdAtCursor", newCursor)
              graph.variables.set(s"integrityCheckState-$name-period", math.max(period, 1))
            }
          }
          None
        } else
          Some {
            db.tryTransaction { implicit graph =>
              Try(globalCheck(service.getByIds(ids: _*)))
            }.getOrElse(Map("globalFailure" -> 1L))
          }
      }
      .reduceOption(_ <+> _)
      .getOrElse(Map.empty)
    val duration = System.currentTimeMillis() - startAt
    if (killSwitch.continueProcess && System.currentTimeMillis() <= stopAt) // all data has been processed
      db.tryTransaction { implicit graph =>
        Try {
          graph.variables.remove(s"integrityCheckState-$name-createdAtCursor")
          if (createdAtCursor.isEmpty) {
            graph.variables.set(s"integrityCheckState-$name-duration", duration)
            graph.variables.remove(s"integrityCheckState-$name-period")
          }
        }
      }
    result + ("duration" -> duration)
  }
}
