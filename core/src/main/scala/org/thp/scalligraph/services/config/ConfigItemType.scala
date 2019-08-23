package org.thp.scalligraph.services.config

import scala.concurrent.duration.{Duration, FiniteDuration}

import play.api.Configuration
import play.api.libs.json._

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}

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
  implicit val json: ConfigItemType[JsValue]                  = build[JsValue]("json")
  implicit val jsObject: ConfigItemType[JsObject]             = build[JsObject]("json object")
  implicit def option[T](implicit cit: ConfigItemType[T]): ConfigItemType[Option[T]] = {
    import cit.format
    build[Option[T]](s"option(${cit.name})")(Reads.optionNoError, Writes.optionWithNull)
  }
  implicit def seq[T](implicit cit: ConfigItemType[T]): ConfigItemType[Seq[T]] = {
    import cit.format
    build[Seq[T]](s"seq(${cit.name})")
  }
  implicit def map[T](implicit cit: ConfigItemType[T]): ConfigItemType[Map[String, T]] = {
    import cit.format
    build[Map[String, T]](s"map(string -> ${cit.name}")
  }
}
