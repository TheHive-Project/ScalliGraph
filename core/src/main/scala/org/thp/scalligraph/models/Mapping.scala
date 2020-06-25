package org.thp.scalligraph.models

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.{Base64, Date}

import gremlin.scala.dsl.Converter
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.Permission
import org.thp.scalligraph.macros.MappingMacro
import org.thp.scalligraph.utils.Hash
import play.api.libs.json._

import scala.language.experimental.macros
import scala.reflect.{classTag, ClassTag}

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
  def toMapping: Mapping[D, _, _] = this
}

trait MappingLowestPrio {
  implicit def build[T]: UniMapping[T] = macro MappingMacro.getOrBuildMapping[T]
}

trait MappingLowerPrio extends MappingLowestPrio {
  implicit val json: SingleMapping[JsValue, String] =
    SingleMapping[JsValue, String](toGraphOptFn = j => Some(j.toString), toDomainFn = s => Json.parse(s))
}

object UniMapping extends MappingLowerPrio {
  implicit val jsObject: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String](toGraphOptFn = j => Some(j.toString), toDomainFn = s => Json.parse(s).as[JsObject])
  val id: SingleMapping[String, AnyRef]                      = SingleMapping[String, AnyRef](toGraphOptFn = i => Some(i), toDomainFn = _.toString)
  implicit val string: SingleMapping[String, String]         = SingleMapping[String, String]()
  implicit val long: SingleMapping[Long, Long]               = SingleMapping[Long, Long]()
  implicit val int: SingleMapping[Int, Int]                  = SingleMapping[Int, Int]()
  implicit val date: SingleMapping[Date, Date]               = SingleMapping[Date, Date]()
  implicit val boolean: SingleMapping[Boolean, Boolean]      = SingleMapping[Boolean, Boolean]()
  implicit val double: SingleMapping[Double, Double]         = SingleMapping[Double, Double]()
  implicit val float: SingleMapping[Float, Float]            = SingleMapping[Float, Float]()
  implicit val permission: SingleMapping[Permission, String] = SingleMapping[Permission, String]()
  implicit val hash: SingleMapping[Hash, String] = SingleMapping[Hash, String](
    toGraphOptFn = h => Some(h.toString),
    toDomainFn = Hash(_)
  )
  implicit val binary: SingleMapping[Array[Byte], String] = SingleMapping[Array[Byte], String](
    toGraphOptFn = b => Some(Base64.getEncoder.encodeToString(b)),
    toDomainFn = s => Base64.getDecoder.decode(s)
  )
  implicit def option[D, G](implicit subMapping: SingleMapping[D, G]): OptionMapping[D, G] = subMapping.optional
  implicit def seq[D, G](implicit subMapping: SingleMapping[D, G]): ListMapping[D, G]      = subMapping.sequence
  implicit def set[D, G](implicit subMapping: SingleMapping[D, G]): SetMapping[D, G]       = subMapping.set
  def enum[E <: Enumeration](e: E): SingleMapping[E#Value, String]                         = SingleMapping[E#Value, String](e => Some(e.toString), e.withName(_))

  val jlong: SingleMapping[Long, JLong]          = SingleMapping[Long, JLong]() //, toGraphOptFn = l => Some(l), toDomainFn = _.longValue())
  val jint: SingleMapping[Int, JInt]             = SingleMapping[Int, JInt]()
  val jdouble: SingleMapping[Double, JDouble]    = SingleMapping[Double, JDouble]()
  val jfloat: SingleMapping[Float, JFloat]       = SingleMapping[Float, JFloat]()
  def identity[N: ClassTag]: SingleMapping[N, N] = SingleMapping[N, N]()
  def jsonNative: SingleMapping[JsValue, Any] =
    SingleMapping[JsValue, Any](
      toGraphOptFn = {
        case JsString(s)  => Some(s)
        case JsBoolean(b) => Some(b)
        case JsNumber(v)  => Some(v)
        case _            => None
      },
      toDomainFn = {
        case s: String  => JsString(s)
        case b: Boolean => JsBoolean(b)
        case d: Date    => JsNumber(d.getTime)
        case n: Number  => JsNumber(n.doubleValue())
        case _          => JsNull
      }
    )

}

/*sealed*/
abstract class Mapping[D, SD: ClassTag, G: ClassTag] extends UniMapping[D] with Converter[SD] {
  override type GraphType = G
  val graphTypeClass: Class[_]  = Option(classTag[G]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val domainTypeClass: Class[_] = Option(classTag[SD]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val cardinality: MappingCardinality.Value
//  val noValue: G
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

  def convertToJava(c: Class[_]): Class[_] = c match {
    case JByte.TYPE     => classOf[JByte]
    case JShort.TYPE    => classOf[Short]
    case Character.TYPE => classOf[Character]
    case Integer.TYPE   => classOf[Integer]
    case JLong.TYPE     => classOf[JLong]
    case JFloat.TYPE    => classOf[JFloat]
    case JDouble.TYPE   => classOf[JDouble]
    case JBoolean.TYPE  => classOf[JBoolean]
    case _              => c
  }
}

case class OptionMapping[D: ClassTag, G: ClassTag](
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
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[D, D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SingleMapping[D, G]         = copy(isReadonly = true)
  override def optional: OptionMapping[D, G]         = OptionMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
  override def sequence: ListMapping[D, G]           = ListMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
  override def set: SetMapping[D, G]                 = SetMapping[D, G](toGraphOptFn, toDomainFn, isReadonly)
}

case class ListMapping[D: ClassTag, G: ClassTag](
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
    toGraphOptFn: D => Option[G] = (d: D) => Some(d.asInstanceOf[G]),
    toDomainFn: G => D = (g: G) => g.asInstanceOf[D],
    isReadonly: Boolean = false
) extends Mapping[Set[D], D, G] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.set
  override def toGraphOpt(d: D): Option[G]           = toGraphOptFn(d)
  override def toDomain(g: G): D                     = toDomainFn(g)
  override def readonly: SetMapping[D, G]            = copy(isReadonly = true)
}
