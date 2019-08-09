package org.thp.scalligraph.services.config

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import play.api.libs.json._
import play.api.{ConfigLoader, Configuration, Logger}

import akka.actor.{ActorRef, ActorSystem}
import javax.inject.{Inject, Named, Singleton}
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.services.EventSrv

@Singleton
class ApplicationConfig @Inject()(
    configuration: Configuration,
    db: Database,
    system: ActorSystem,
    eventSrv: EventSrv,
    @Named("config-actor") configActor: ActorRef,
    implicit val ec: ExecutionContext
) {
  lazy val logger                          = Logger(getClass)
  val ignoreDatabaseConfiguration: Boolean = configuration.get[Boolean]("ignoreDatabaseConfiguration")
  if (ignoreDatabaseConfiguration) logger.warn("Emit warning !") // TODO

  private var items: Map[String, ConfigItemImpl[_]] = Map.empty
  private val itemsLock                             = new Object

  def item[T: ConfigLoader](path: String, description: String)(
      implicit configItemType: ConfigItemType[T]
  ): ConfigItem[T] =
    validatedItem[T](path, description, Success.apply)

  def validatedItem[T: ConfigLoader](path: String, description: String, validation: T => Try[T])(
      implicit configItemType: ConfigItemType[T]
  ): ConfigItem[T] =
    itemsLock.synchronized {
      items
        .getOrElse(
          path, {
            val configItem =
              new ConfigItemImpl(path, description, configuration.get[T](path), configItemType, validation, db, eventSrv, configActor, ec)
            items = items + (path -> configItem)
            configItem
          }
        )
        .asInstanceOf[ConfigItem[T]]
    }

  def context[C](context: ConfigContext[C]): ContextApplicationConfig[C] =
    new ContextApplicationConfig[C](context, configuration, db, eventSrv, configActor, ec)

  def list: Seq[ConfigItem[_]] = items.values.toSeq

  def set(path: String, value: JsValue)(implicit authContext: AuthContext): Try[Unit] = items.get(path) match {
    case Some(i) => i.setJson(value)
    case None    => Failure(NotFoundError(s"Configuration $path not found"))
  }
}

class ContextApplicationConfig[C](
    context: ConfigContext[C],
    configuration: Configuration,
    db: Database,
    eventSrv: EventSrv,
    configActor: ActorRef,
    implicit val ec: ExecutionContext
) {
  lazy val logger = Logger(getClass)

  def item[T: ConfigLoader](path: String, description: String)(
      implicit configItemType: ConfigItemType[T]
  ): ContextConfigItem[T, C] =
    validatedItem[T](path, description, Success.apply)

  def validatedItem[T: ConfigLoader](path: String, description: String, validation: T => Try[T])(
      implicit configItemType: ConfigItemType[T]
  ): ContextConfigItem[T, C] =
    new ContextConfigItemImpl(
      context,
      path,
      description,
      configuration.get[T](context.defaultPath(path)),
      configItemType,
      validation,
      db,
      eventSrv,
      configActor,
      ec
    )
}
