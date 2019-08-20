package org.thp.scalligraph.models

import java.util.Date

import scala.language.experimental.macros
import scala.reflect.{classTag, ClassTag}

import play.api.libs.json.{JsObject, JsValue, Json}

import gremlin.scala.dsl.Converter
import org.thp.scalligraph.auth.Permission
import org.thp.scalligraph.macros.MappingMacro
import org.thp.scalligraph.{Hash, InternalError, Utils}

object MappingCardinality extends Enumeration {
  val option, single, list, set = Value

  def isCompatible(c1: Value, c2: Value): Boolean =
    c1 == c2 ||
      (c1 == single && c2 == option) ||
      (c1 == option && c2 == single)
}

trait UniMapping[D] {
  _: Mapping[D, _, _] =>
  type GraphType
}

trait MappingLowestPrio {
  implicit def build[T]: UniMapping[T] = macro MappingMacro.getOrBuildMapping[T]
}

trait MappingLowerPrio extends MappingLowestPrio {
  implicit val json: SingleMapping[JsValue, String] =
    SingleMapping[JsValue, String]("", toGraphOptFn = j => Some(j.toString), toDomainFn = s => Json.parse(s))
}

object UniMapping extends MappingLowerPrio {
  implicit val jsObject: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String]("", toGraphOptFn = j => Some(j.toString), toDomainFn = s => Json.parse(s).as[JsObject])
  implicit val string: SingleMapping[String, String]         = SingleMapping[String, String]("")
  implicit val long: SingleMapping[Long, Long]               = SingleMapping[Long, Long](0)
  implicit val int: SingleMapping[Int, Int]                  = SingleMapping[Int, Int](0)
  implicit val date: SingleMapping[Date, Date]               = SingleMapping[Date, Date](new Date(0))
  implicit val boolean: SingleMapping[Boolean, Boolean]      = SingleMapping[Boolean, Boolean](false)
  implicit val double: SingleMapping[Double, Double]         = SingleMapping[Double, Double](0)
  implicit val float: SingleMapping[Float, Float]            = SingleMapping[Float, Float](0)
  implicit val permission: SingleMapping[Permission, String] = SingleMapping[Permission, String]("")
  implicit val hash: SingleMapping[Hash, String] = SingleMapping[Hash, String](
    "",
    toGraphOptFn = h => Some(h.toString),
    toDomainFn = Hash(_)
  )
  implicit def option[D, G](implicit subMapping: SingleMapping[D, G]): OptionMapping[D, G] = subMapping.optional
  implicit def seq[D, G](implicit subMapping: SingleMapping[D, G]): ListMapping[D, G]      = subMapping.sequence
  implicit def set[D, G](implicit subMapping: SingleMapping[D, G]): SetMapping[D, G]       = subMapping.set
  def enum[E <: Enumeration](e: E): SingleMapping[E#Value, String]                         = SingleMapping[E#Value, String]("", e => Some(e.toString), e.withName(_))
}

sealed abstract class Mapping[D, SD: ClassTag, G: ClassTag] extends UniMapping[D] with Converter[SD] {
  override type GraphType = G
  val graphTypeClass: Class[_]  = Utils.convertToJava(classTag[G].runtimeClass)
  val domainTypeClass: Class[_] = Utils.convertToJava(classTag[SD].runtimeClass)
  val cardinality: MappingCardinality.Value
  val noValue: G
  val isReadonly: Boolean
  override def toGraph(d: SD): G = toGraphOpt(d).get
  def toGraphOpt(d: SD): Option[G]
  override def toDomain(g: G): SD
  def sequence: Mapping[Seq[D], D, G]    = throw InternalError(s"Sequence of $this is not supported")
  def set: Mapping[Set[D], D, G]         = throw InternalError(s"Set of $this is not supported")
  def optional: Mapping[Option[D], D, G] = throw InternalError(s"Option of $this is not supported")
  def readonly: Mapping[D, SD, G]

  def isCompatibleWith(m: Mapping[_, _, _]): Boolean =
    graphTypeClass.equals(m.graphTypeClass) && MappingCardinality.isCompatible(cardinality, m.cardinality)
}

case class OptionMapping[D: ClassTag, G: ClassTag](
    noValue: G,
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Option[D], D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.option
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: OptionMapping[D, G]         = copy(isReadonly = true)
}

case class SingleMapping[D: ClassTag, G: ClassTag](
    noValue: G,
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[D, D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SingleMapping[D, G]         = copy(isReadonly = true)
  override def optional: OptionMapping[D, G]         = OptionMapping[D, G](noValue, toGraphOptFn, toDomainFn, isReadonly)
  override def sequence: ListMapping[D, G]           = ListMapping[D, G](noValue, toGraphOptFn, toDomainFn, isReadonly)
  override def set: SetMapping[D, G]                 = SetMapping[D, G](noValue, toGraphOptFn, toDomainFn, isReadonly)
}

case class ListMapping[D: ClassTag, G: ClassTag](
    noValue: G,
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Seq[D], D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.list
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: ListMapping[D, G]           = copy(isReadonly = true)
}

case class SetMapping[D: ClassTag, G: ClassTag](
    noValue: G,
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Set[D], D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.set
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SetMapping[D, G]            = copy(isReadonly = true)
}
