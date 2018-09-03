package org.thp.scalligraph.models

import java.util.Date

import gremlin.scala.dsl.Converter
import play.api.Logger
import play.api.libs.json.{JsObject, Json}

object Schema {
  val logger = Logger(getClass)
}

trait UniMapping[D] {
  _: Mapping[D, _, _] ⇒
}

object MappingCardinality extends Enumeration {
  val option, single, list, set = Value
  def isCompatible(c1: Value, c2: Value): Boolean =
    c1 == c2 ||
      (c1 == single && c2 == option) ||
      (c1 == option && c2 == single)
}
sealed trait Mapping[D, SD, G] extends UniMapping[D] with Converter[SD] {
  override type GraphType = G
  val graphTypeClass: Class[_]
  val cardinality: MappingCardinality.Value
  val isReadonly: Boolean
  override def toGraph(d: SD): G = toGraphOpt(d).get
  def toGraphOpt(d: SD): Option[G]
  override def toDomain(g: G): SD
  def sequence: Mapping[Seq[D], D, G]    = ???
  def set: Mapping[Set[D], D, G]         = ???
  def optional: Mapping[Option[D], D, G] = ???
  def readonly: Mapping[D, SD, G]
  def isCompatibleWith(m: Mapping[_, _, _]): Boolean =
    graphTypeClass.equals(m.graphTypeClass) && MappingCardinality.isCompatible(cardinality, m.cardinality)
}

case class OptionMapping[D, G](
    graphTypeClass: Class[_],
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Option[D], D, G] {
  override val cardinality                   = MappingCardinality.option
  override def toGraphOpt(d: D): Option[G]   = toGraphOptFn(d)
  override def toDomain(g: G): D             = toDomainFn(g)
  override def readonly: OptionMapping[D, G] = copy(isReadonly = true)
}

case class SingleMapping[D, G](
    graphTypeClass: Class[_],
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[D, D, G] {
  override val cardinality                   = MappingCardinality.single
  override def toGraphOpt(d: D): Option[G]   = toGraphOptFn(d)
  override def toDomain(g: G): D             = toDomainFn(g)
  override def readonly: SingleMapping[D, G] = copy(isReadonly = true)
  override def optional: OptionMapping[D, G] = OptionMapping[D, G](graphTypeClass, toGraphOptFn, toDomainFn, isReadonly)
  override def sequence: ListMapping[D, G]   = ListMapping[D, G](graphTypeClass, toGraphOptFn, toDomainFn, isReadonly)
  override def set: SetMapping[D, G]         = SetMapping[D, G](graphTypeClass, toGraphOptFn, toDomainFn, isReadonly)
}

case class ListMapping[D, G](
    graphTypeClass: Class[_],
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Seq[D], D, G] {
  override val cardinality                 = MappingCardinality.list
  override def toGraphOpt(d: D): Option[G] = toGraphOptFn(d)
  override def toDomain(g: G): D           = toDomainFn(g)
  override def readonly: ListMapping[D, G] = copy(isReadonly = true)
}

case class SetMapping[D, G](
    graphTypeClass: Class[_],
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Set[D], D, G] {
  override val cardinality                 = MappingCardinality.set
  override def toGraphOpt(d: D): Option[G] = toGraphOptFn(d)
  override def toDomain(g: G): D           = toDomainFn(g)
  override def readonly: SetMapping[D, G]  = copy(isReadonly = true)

}

object UniMapping {
  implicit val stringMapping: SingleMapping[String, String] = SingleMapping[String, String](classOf[String])
  implicit val longMapping: SingleMapping[Long, Long] =
    SingleMapping[Long, Long](classOf[Long])
  implicit val intMapping: SingleMapping[Int, Int] =
    SingleMapping[Int, Int](classOf[Int])
  implicit val dateMapping: SingleMapping[Date, Date] =
    SingleMapping[Date, Date](classOf[Date])
  implicit val booleanMapping: SingleMapping[Boolean, Boolean] =
    SingleMapping[Boolean, Boolean](classOf[Boolean])
  implicit val jsonMapping: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String](classOf[String], toGraphOptFn = j ⇒ Some(j.toString), toDomainFn = s ⇒ Json.parse(s).as[JsObject])
  implicit val doubleMapping: SingleMapping[Double, Double] =
    SingleMapping[Double, Double](classOf[Double])
  implicit val floatMapping: SingleMapping[Float, Float] =
    SingleMapping[Float, Float](classOf[Float])
  implicit def optionMapping[D, G](implicit subMapping: SingleMapping[D, G]): OptionMapping[D, G] = subMapping.optional
}
