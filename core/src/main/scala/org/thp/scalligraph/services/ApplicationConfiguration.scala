package org.thp.scalligraph.services

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.{BadConfigurationError, NotFoundError}
import play.api.libs.json._
import play.api.{ConfigLoader, Configuration, Logger}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

trait ConfigItemType[T] {
  implicit val format: Format[T]
  val name: String
}

object ConfigItemType {

  implicit val finiteDurationReads: Reads[FiniteDuration] = Reads[FiniteDuration] {
    case JsString(Duration(n, u)) => JsSuccess(FiniteDuration(n, u))
    case other                    => JsError(s"$other is not a valid duration")
  }
  implicit val finiteDurationWrites: Writes[FiniteDuration] = Writes[FiniteDuration](d => JsString(d.toString))
  implicit val configurationReads: Reads[Configuration] =
    Reads[Configuration](json => JsSuccess(Configuration(ConfigFactory.parseString(json.toString))))

  implicit val configurationWrites: Writes[Configuration] =
    Writes[Configuration](cfg => Json.parse(cfg.underlying.root().render(ConfigRenderOptions.concise())))

  def build[T](name0: String)(implicit jsReads: Reads[T], jsWrites: Writes[T]): ConfigItemType[T] = new ConfigItemType[T] {
    override val name: String      = name0
    override val format: Format[T] = Format(jsReads, jsWrites)
  }
  implicit val finiteDuration: ConfigItemType[FiniteDuration] = build[FiniteDuration]("duration")
  implicit val boolean: ConfigItemType[Boolean]               = build[Boolean]("boolean")
  implicit val string: ConfigItemType[String]                 = build[String]("string")
  implicit val configuration: ConfigItemType[Configuration]   = build[Configuration]("configuration")
  implicit def option[T](implicit cit: ConfigItemType[T]): ConfigItemType[Option[T]] = {
    import cit.format
    build[Option[T]](s"option(${cit.name})")(Reads.optionNoError, Writes.optionWithNull)
  }
  implicit def seq[T](implicit cit: ConfigItemType[T]): ConfigItemType[Seq[T]] = {
    import cit.format
    build[Seq[T]](s"seq(${cit.name})")
  }
}

trait ConfigItem[T] {
  val path: String
  val description: String
  val defaultValue: T
  val configItemType: ConfigItemType[T]
  def get: T
  def set(v: T): Try[Unit]
  def validation(v: T): Try[T]
  def getDefaultValueJson: JsValue = configItemType.format.writes(defaultValue)
  def getJson: JsValue             = configItemType.format.writes(get)

  def setJson(v: JsValue): Try[Unit] =
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

@Singleton
class ApplicationConfiguration @Inject()(configuration: Configuration, db: Database) {
  lazy val logger                          = Logger(getClass)
  val ignoreDatabaseConfiguration: Boolean = configuration.get[Boolean]("ignoreDatabaseConfiguration")
  if (ignoreDatabaseConfiguration) logger.warn("Emit warning !") // TODO

  class MutableConfigItem[T](
      val path: String,
      val description: String,
      val defaultValue: T,
      private var value: T,
      val configItemType: ConfigItemType[T],
      val validationFunction: T => Try[T],
      private val setter: T => Unit,
      private var updateCallbacks: List[(T, T) => Unit] = Nil
  ) extends ConfigItem[T] {

    private[ApplicationConfiguration] def update(newValueJson: JsValue): JsResult[Unit] = {
      val oldValue = value
      configItemType.format.reads(newValueJson).map { newValue =>
        value = newValue
        updateCallbacks.foreach(_.apply(oldValue, value))
      }
    }
    override def get: T                   = value
    override def set(v: T): Try[Unit]     = validation(v).map(setter)
    override def validation(v: T): Try[T] = validationFunction(v)
    override def onUpdate(f: (T, T) => Unit): Unit = synchronized {
      updateCallbacks = f :: updateCallbacks
    }
  }

  private var items: Map[String, MutableConfigItem[_]] = Map.empty
  private val itemsLock                                = new Object

  private def itemSetter[T](path: String)(implicit configItemType: ConfigItemType[T]): T => Unit =
    (value: T) => {
      val valueJson = configItemType.format.writes(value)
      db.noTransaction { implicit graph =>
        graph
          .variables()
          .set(s"config.$path", valueJson)
      }
      // TODO notify all MutableConfiguration on the cluster
      update(path, valueJson)
    }

  private def update(path: String, value: JsValue): Unit = items.get(path).foreach(_.update(value))

  private def getValue(path: String): Option[JsValue] =
    if (ignoreDatabaseConfiguration) None
    else
      db.noTransaction { implicit graph =>
        graph
          .variables()
          .get[String](s"config.$path")
          .asScala
          .flatMap(s => Try(Json.parse(s)).toOption)
      }

  def item[T: ConfigLoader](path: String, description: String)(implicit configItemType: ConfigItemType[T]): ConfigItem[T] =
    validatedItem[T](path, description, Success.apply)

  def validatedItem[T: ConfigLoader](path: String, description: String, validation: T => Try[T])(
      implicit configItemType: ConfigItemType[T]
  ): ConfigItem[T] = {
    val defaultValue = configuration.get[T](path)
    itemsLock.synchronized {
      items
        .getOrElse(
          path, {
            val value = getValue(path)
              .flatMap(configItemType.format.reads(_).asOpt)
              .getOrElse(defaultValue)
            val configItem = new MutableConfigItem(path, description, defaultValue, value, configItemType, validation, itemSetter(path))
            items = items + (path -> configItem)
            configItem
          }
        )
        .asInstanceOf[ConfigItem[T]]
    }
  }

  def list: Seq[ConfigItem[_]] = items.values.toSeq

  def set(path: String, value: JsValue): Try[Unit] = items.get(path) match {
    case Some(i) => i.setJson(value)
    case None    => Failure(NotFoundError(s"Configuration $path not found"))
  }
}
