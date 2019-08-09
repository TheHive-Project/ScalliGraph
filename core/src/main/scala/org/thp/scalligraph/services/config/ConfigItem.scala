package org.thp.scalligraph.services.config
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}

import akka.actor.ActorRef
import akka.pattern.ask
import org.thp.scalligraph.BadConfigurationError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.services.EventSrv

trait ConfigItem[T] {
  val path: String
  val description: String
  val defaultValue: T
  val configItemType: ConfigItemType[T]
  def get: T
  def set(v: T)(implicit authContext: AuthContext): Try[Unit]
  def validation(v: T): Try[T]
  def getDefaultValueJson: JsValue = configItemType.format.writes(defaultValue)
  def getJson: JsValue             = configItemType.format.writes(get)

  def setJson(v: JsValue)(implicit authContext: AuthContext): Try[Unit] =
    configItemType
      .format
      .reads(v)
      .map(set)
      .fold(
        error => {
          val message = JsObject(error.map {
            case (path, es) => path.toString -> Json.toJson(es.flatMap(_.messages))
          })
          Failure(BadConfigurationError(message.toString))
        },
        identity
      )
  def onUpdate(f: (T, T) => Unit): Unit
}

class ConfigItemImpl[T](
    val path: String,
    val description: String,
    val defaultValue: T,
    val configItemType: ConfigItemType[T],
    val validationFunction: T => Try[T],
    db: Database,
    eventSrv: EventSrv,
    configActor: ActorRef,
    implicit val ec: ExecutionContext
) extends ConfigItem[T] {
  lazy val logger                                   = Logger(getClass)
  private var value: T                              = _
  @volatile private var flag                        = false
  private var updateCallbacks: List[(T, T) => Unit] = Nil

  invalidateCache(Success(()))

  private def invalidateCache(msg: Try[Any]): Unit = {
    msg.foreach {
      case Invalidate(_) =>
        val oldValue = value
        value = getValue.getOrElse(defaultValue)
        updateCallbacks.foreach(_.apply(oldValue, value))
      case _ =>
    }
    configActor
      .ask(WaitNotification(path))(1.hour)
      .onComplete(invalidateCache)
  }

  protected def getValue: Option[T] =
    db.roTransaction { implicit graph =>
        graph
          .variables()
          .get[String](s"config.$path")
      }
      .asScala
      .flatMap(s => Try(Json.parse(s)).toOption)
      .flatMap(configItemType.format.reads(_).asOpt)

  override def get: T = {
    if (!flag)
      synchronized {
        if (!flag)
          value = getValue.getOrElse(defaultValue)
        flag = true
      }
    value
  }

  override def set(v: T)(implicit authContext: AuthContext): Try[Unit] = validation(v).flatMap { value =>
    val valueJson = configItemType.format.writes(value)
    db.tryTransaction { implicit graph =>
        Try(
          graph
            .variables()
            .set(s"config.$path", valueJson)
        )
      }
      .map(_ => eventSrv.publish(ConfigTopic.topicName)(Invalidate(path)))
  }

  override def validation(v: T): Try[T] = validationFunction(v)

  override def onUpdate(f: (T, T) => Unit): Unit = synchronized {
    updateCallbacks = f :: updateCallbacks
  }
}
