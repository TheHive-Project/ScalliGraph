package org.thp.scalligraph.models

import java.util.Date

import scala.reflect.{classTag, ClassTag}

import play.api.libs.json.{JsObject, Json}

import gremlin.scala.dsl.Converter

object MappingCardinality extends Enumeration {
  val option, single, list, set = Value
  def isCompatible(c1: Value, c2: Value): Boolean =
    c1 == c2 ||
      (c1 == single && c2 == option) ||
      (c1 == option && c2 == single)
}

trait UniMapping[D] {
  _: Mapping[D, _, _] ⇒
  type GraphType
}

object UniMapping {
  implicit val jsonMapping: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String](toGraphOptFn = j ⇒ Some(j.toString), toDomainFn = s ⇒ Json.parse(s).as[JsObject])
  implicit val stringMapping: SingleMapping[String, String]                                       = SingleMapping[String, String]()
  implicit val longMapping: SingleMapping[Long, Long]                                             = SingleMapping[Long, Long]()
  implicit val intMapping: SingleMapping[Int, Int]                                                = SingleMapping[Int, Int]()
  implicit val dateMapping: SingleMapping[Date, Date]                                             = SingleMapping[Date, Date]()
  implicit val booleanMapping: SingleMapping[Boolean, Boolean]                                    = SingleMapping[Boolean, Boolean]()
  implicit val doubleMapping: SingleMapping[Double, Double]                                       = SingleMapping[Double, Double]()
  implicit val floatMapping: SingleMapping[Float, Float]                                          = SingleMapping[Float, Float]()
  implicit def optionMapping[D, G](implicit subMapping: SingleMapping[D, G]): OptionMapping[D, G] = subMapping.optional
  implicit def seqMapping[D, G](implicit subMapping: SingleMapping[D, G]): ListMapping[D, G]      = subMapping.sequence
  implicit def setMapping[D, G](implicit subMapping: SingleMapping[D, G]): SetMapping[D, G]       = subMapping.set
}

sealed trait Mapping[D, SD, G] extends UniMapping[D] with Converter[SD] {
  override type GraphType = G
  val graphTypeClass: Class[_]
  val domainTypeClass: Class[_]
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

case class OptionMapping[D: ClassTag, G: ClassTag](
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Option[D], D, G] {
  override val graphTypeClass: Class[_]              = classTag[G].runtimeClass
  override val domainTypeClass: Class[_]             = classTag[D].runtimeClass
  override val cardinality: MappingCardinality.Value = MappingCardinality.option
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: OptionMapping[D, G]         = copy(isReadonly = true)
}

case class SingleMapping[D: ClassTag, G: ClassTag](
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[D, D, G] {
  override val graphTypeClass: Class[_]              = classTag[G].runtimeClass
  override val domainTypeClass: Class[_]             = classTag[D].runtimeClass
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SingleMapping[D, G]         = copy(isReadonly = true)
  override def optional: OptionMapping[D, G]         = OptionMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
  override def sequence: ListMapping[D, G]           = ListMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
  override def set: SetMapping[D, G]                 = SetMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
}

case class ListMapping[D: ClassTag, G: ClassTag](
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Seq[D], D, G] {
  override val graphTypeClass: Class[_]              = classTag[G].runtimeClass
  override val domainTypeClass: Class[_]             = classTag[D].runtimeClass
  override val cardinality: MappingCardinality.Value = MappingCardinality.list
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: ListMapping[D, G]           = copy(isReadonly = true)
}

case class SetMapping[D: ClassTag, G: ClassTag](
    toGraphOptFn: D ⇒ Option[G] = (d: D) ⇒ Some(d.asInstanceOf[G]),
    toDomainFn: G ⇒ D = (g: G) ⇒ g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Set[D], D, G] {
  override val graphTypeClass: Class[_]              = classTag[G].runtimeClass
  override val domainTypeClass: Class[_]             = classTag[D].runtimeClass
  override val cardinality: MappingCardinality.Value = MappingCardinality.set
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SetMapping[D, G]            = copy(isReadonly = true)
}
