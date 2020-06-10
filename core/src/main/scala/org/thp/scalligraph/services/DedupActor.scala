package org.thp.scalligraph.services

import java.util.{Map => JMap}

import akka.actor.{Actor, Cancellable}
import gremlin.scala._
import org.thp.scalligraph.models.{Database, Entity, IndexType}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps
import play.api.Logger

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.Ordering
import scala.util.Try

object DedupActor {
  final case object EntityAdded
}

trait DedupOps[E <: Product] {
  val db: Database
  val service: VertexSrv[E, _ <: VertexSteps[E]]
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

  def copyEdge(from: E with Entity, to: E with Entity)(implicit graph: Graph): Unit = {
    val toVertex: Vertex = graph.V(to._id).head
    service.get(from).raw.outE().toList().foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => Key(p.key()) -> p.value()).toList
      val label = edge.label()
      logger.debug(s"create edge from $toVertex to ${graph.E(edge.id()).inV().head()} with properties: $props")
      graph.E(edge.id()).inV().addE(label, props: _*).from(toVertex).iterate()
    }
    service.get(from).raw.inE().toList().foreach { edge =>
      val props = edge.properties[Any]().asScala.map(p => Key(p.key()) -> p.value()).toList
      val label = edge.label()
      logger.debug(s"create edge from ${graph.E(edge.id()).outV().head()} to $toVertex with properties: $props")
      graph.E(edge.id()).outV().addE(label, props: _*).to(toVertex).iterate()
    }
  }

  val createdFirst: Ordering[E with Entity] = (x: E with Entity, y: E with Entity) => x._createdAt.compareTo(y._createdAt)

  val updatedFirst: Ordering[E with Entity] = (x: E with Entity, y: E with Entity) =>
    x._updatedAt.getOrElse(x._createdAt).compareTo(y._updatedAt.getOrElse(y._createdAt))
}

abstract class DedupActor[E <: Product] extends Actor with DedupOps[E] {
  import DedupActor._
  final case object NeedCheck
  final case object Check

  val min: FiniteDuration
  val max: FiniteDuration
  def resolve(entities: List[E with Entity])(implicit graph: Graph): Try[Unit]

  lazy val uniqueProperties: Seq[String] = service.model.indexes.flatMap {
    case (IndexType.unique, properties) if properties.lengthCompare(1) == 0 => properties
    case (IndexType.unique, _) =>
      logger.warn("Index on only one property can be deduplicated")
      None
    case _ => Nil
  }

  def check(): Unit =
    uniqueProperties
      .map(getDuplicates)
      .foreach(_.foreach { entities =>
        db.tryTransaction { implicit graph =>
          resolve(entities)
        }
      })

  override def receive: Receive = receive(needCheck = false, None)
  def receive(needCheck: Boolean, timer: Option[Cancellable]): Receive = {
    case EntityAdded =>
      context.system.scheduler.scheduleOnce(min, self, NeedCheck)(context.system.dispatcher)
      ()
    case NeedCheck if !needCheck =>
      context.become(
        receive(
          needCheck = true,
          timer orElse Some(context.system.scheduler.scheduleAtFixedRate(Duration.Zero, max, self, Check)(context.system.dispatcher))
        )
      )
    case Check if needCheck =>
      check()
      context.become(receive(needCheck = false, timer))
    case Check =>
      timer.foreach(_.cancel())
      context.become(receive(needCheck = false, None))
  }

}
