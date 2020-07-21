package org.thp.scalligraph.models

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.{Base64, Date, UUID}

import gremlin.scala.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.Permission
import org.thp.scalligraph.controllers.Renderer
import org.thp.scalligraph.macros.MappingMacro
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{BiConverter, Converter, Traversal}
import org.thp.scalligraph.utils.Hash
import play.api.libs.json._

import scala.collection.JavaConverters._
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
    SingleMapping[JsValue, String](toGraph = j => j.toString, toDomain = s => Json.parse(s))
}

object UniMapping extends MappingLowerPrio {
  implicit val permissionWrites: Writes[Permission] = Writes.stringableWrites(identity)
  implicit val jsObject: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String](toGraph = j => j.toString, toDomain = s => Json.parse(s).as[JsObject])
  val id: SingleMapping[String, AnyRef]                      = SingleMapping[String, AnyRef](toGraph = i => Some(i), toDomain = _.toString)
  implicit val string: SingleMapping[String, String]         = SingleMapping[String, String]()
  implicit val long: SingleMapping[Long, Long]               = SingleMapping[Long, Long]()
  implicit val int: SingleMapping[Int, Int]                  = SingleMapping[Int, Int]()
  implicit val date: SingleMapping[Date, Date]               = SingleMapping[Date, Date]()
  implicit val boolean: SingleMapping[Boolean, Boolean]      = SingleMapping[Boolean, Boolean]()
  implicit val double: SingleMapping[Double, Double]         = SingleMapping[Double, Double]()
  implicit val float: SingleMapping[Float, Float]            = SingleMapping[Float, Float]()
  implicit val permission: SingleMapping[Permission, String] = SingleMapping[Permission, String]()
  implicit val hash: SingleMapping[Hash, String] = SingleMapping[Hash, String](
    toGraph = h => h.toString,
    toDomain = Hash(_)
  )
  implicit val binary: SingleMapping[Array[Byte], String] = SingleMapping[Array[Byte], String](
    toGraph = b => Base64.getEncoder.encodeToString(b),
    toDomain = s => Base64.getDecoder.decode(s)
  )
  implicit def option[D, G](implicit subMapping: SingleMapping[D, G]): OptionMapping[D, G] = subMapping.optional
  implicit def seq[D, G](implicit subMapping: SingleMapping[D, G]): ListMapping[D, G]      = subMapping.sequence
  implicit def set[D, G](implicit subMapping: SingleMapping[D, G]): SetMapping[D, G]       = subMapping.set
  def enum[E <: Enumeration](e: E): SingleMapping[E#Value, String]                         = SingleMapping[E#Value, String](_.toString, e.withName(_))

  val jlong: SingleMapping[Long, JLong]       = SingleMapping[Long, JLong]() //, toGraph = l => Some(l), toDomain= _.longValue())
  val jint: SingleMapping[Int, JInt]          = SingleMapping[Int, JInt]()
  val jdouble: SingleMapping[Double, JDouble] = SingleMapping[Double, JDouble]()
  val jfloat: SingleMapping[Float, JFloat]    = SingleMapping[Float, JFloat]()
//  def identity[N: ClassTag]: SingleMapping[N, N] = SingleMapping[N, N]()
  def jsonNative: SingleMapping[JsValue, Any] =
    SingleMapping[JsValue, Any](
      toGraph = {
        case JsString(s)  => Some(s)
        case JsBoolean(b) => Some(b)
        case JsNumber(v)  => Some(v)
        case _            => None
      },
      toDomain = {
        case s: String  => JsString(s)
        case b: Boolean => JsBoolean(b)
        case d: Date    => JsNumber(d.getTime)
        case n: Number  => JsNumber(n.doubleValue())
        case _          => JsNull
      }
    )

}

/*sealed*/
/*
create SD => G
update SD => G
get G => SD
filter SD => G
 */
abstract class Mapping[M, D: ClassTag, G: ClassTag](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D]
)(implicit val getRenderer: Renderer[M], val selectRenderer: Renderer[D])
    extends UniMapping[M]
    with BiConverter[D, G] {
  override type GraphType = G
  val graphTypeClass: Class[_]  = Option(classTag[G]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val domainTypeClass: Class[_] = Option(classTag[D]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val cardinality: MappingCardinality.Value
//  val noValue: G
  val isReadonly: Boolean

  override def apply(g: G): D           = toDomain(g)
  override val reverse: Converter[G, D] = toGraph(_)

  def sequence: Mapping[Seq[M], M, G]    = throw InternalError(s"Sequence of $this is not supported")
  def set: Mapping[Set[M], M, G]         = throw InternalError(s"Set of $this is not supported")
  def optional: Mapping[Option[M], M, G] = throw InternalError(s"Option of $this is not supported")
  def readonly: Mapping[M, D, G]

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

  def getProperty(element: Element, key: String): M
  def setProperty(element: Element, key: String, value: M): Unit
  def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](traversal: Traversal[TD, TG, TC], key: String, value: M): Traversal[TD, TG, TC]

  def wrap(us: Seq[D]): M
//  def getProperty[TD, TG <: Element, C <: Converter[M, JList[G]]](traversal: Traversal[TD, TG, _], key: String): Traversal[M, JList[G], C]
//  def selectProperty[TD, TG <: Element, TC <: Converter[TD, TG]](traversal: Traversal[TD, TG, TC], key: String): Traversal[D, G, Converter[D, G]]
}

case class SingleMapping[D: ClassTag, G: ClassTag](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[D, D, G](toGraph, toDomain) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
  override def readonly: SingleMapping[D, G]         = copy(isReadonly = true)
  override def optional: OptionMapping[D, G]         = OptionMapping[D, G](toGraph, toDomain, isReadonly)
  override def sequence: ListMapping[D, G]           = ListMapping[D, G](toGraph, toDomain, isReadonly)
  override def set: SetMapping[D, G]                 = SetMapping[D, G](toGraph, toDomain, isReadonly)
  //  override def toOutput(d: D): Output[D] = Output(d, implicitly[Renderer[D]].toJson(d))

  def getProperty(element: Element, key: String): D = {
    val values = element.properties[G](key)
    if (values.hasNext) {
      val v = apply(values.next().value)
      if (values.hasNext)
        throw InternalError(s"Property $key must have only one value but is multivalued on element $element" + Model.printElement(element))
      else v
    } else throw InternalError(s"Property $key is missing on element $element" + Model.printElement(element))
  }

  def setProperty(element: Element, key: String, value: D): Unit = {
    element.property(key, reverse(value))
    ()
  }

  override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
      traversal: Traversal[TD, TG, TC],
      key: String,
      value: D
  ): Traversal[TD, TG, TC] =
    traversal.onDeepRaw(_.property(key, reverse(value)).iterate())

  override def wrap(us: Seq[D]): D = us.head
  //  override def getProperty[TD, TG <: Element, C <: Converter[D, JList[G]]](traversal: Traversal[TD, TG, _], key: String): Traversal[D, JList[G], C] =
  //    ???
  //
  //  override def selectProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
  //      traversal: Traversal[TD, TG, TC],
  //      key: String
  //  ): Traversal[D, G, Converter[D, G]] = ???
}

case class OptionMapping[D: ClassTag, G: ClassTag](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Option[D], D, G](toGraph, toDomain)(implicitly[ClassTag[D]], implicitly[ClassTag[G]], renderer.opt, renderer) { thisMapping =>
  override val cardinality: MappingCardinality.Value = MappingCardinality.option
  override def readonly: OptionMapping[D, G]         = copy(isReadonly = true)
//  override def toOutput(d: Option[D]): Output[Option[D]] = Output(d, d.fold[JsValue](JsNull)(implicitly[Renderer[D]].toJson(_)))
  override def getProperty(element: Element, key: String): Option[D] = {
    val values = element.properties[G](key)
    if (values.hasNext) {
      val v = apply(values.next().value)
      if (values.hasNext)
        throw InternalError(s"Property $key must have only one value but is multivalued on element $element" + Model.printElement(element))
      else Some(v)
    } else None
  }

  override def setProperty(element: Element, key: String, value: Option[D]): Unit = {
    value match {
      case Some(v) => element.property(key, reverse(v))
      case None    => element.properties(key).forEachRemaining(_.remove())
    }
    ()
  }

  override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
      traversal: Traversal[TD, TG, TC],
      key: String,
      value: Option[D]
  ): Traversal[TD, TG, TC] =
    value.fold(traversal.removeProperty(key))(v => traversal.onDeepRaw(_.property(key, reverse(v)).iterate()))

  override def wrap(us: Seq[D]): Option[D] = us.headOption
//  override def getProperty[TD, TG <: Element, C <: Converter[D, JList[G]]](
//      traversal: Traversal[TD, TG, _],
//      key: String
//  ): Traversal[Option[D], JList[G], C] = ??? //: Traversal[Option[D], JList[G], Converter[Option[D], JList[G]]] = ???
//  override def getProperty[TD, TG <: Element, TC <: Converter[TD, TG]](traversal: Traversal[TD, TG, Converter[TD, TG]], key: String):
//  Traversal[Option[D], JList[G], Poly1Converter[Option[D], JList[G], D, G, Converter[D, G]]] =
//    traversal.property[D, G](key, thisMapping).fold.setConverter(new Poly1Converter[Option[D], JList[G], D, G, Converter[D, G]] {
//      override val subConverter: Converter[D, G] = thisMapping
////override val subConverter: Converter[D, TG] = _
//      override def apply(v: JList[G]): Option[D] = if (v.isEmpty) None else Some(thisMapping(v.get(0)))
//
//    })

//  override def selectProperty[TD, TG <: Element](traversal: Traversal[TD, TG, Converter[TD, TG]], key: String): Traversal[D, G, Converter[D, G]] = ???
}

case class ListMapping[D: ClassTag, G: ClassTag](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Seq[D], D, G](toGraph, toDomain)(implicitly[ClassTag[D]], implicitly[ClassTag[G]], renderer.list, renderer) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.list
  override def readonly: ListMapping[D, G]           = copy(isReadonly = true)

  //  override def toOutput(d: Seq[D]): Output[Seq[D]] = Output(d, JsArray(d.map(implicitly[Renderer[D]].toJson(_))))

  override def getProperty(element: Element, key: String): Seq[D] =
    element
      .properties[G](key)
      .asScala
      .map(p => apply(p.value()))
      .toSeq

  override def setProperty(element: Element, key: String, values: Seq[D]): Unit = {
    element.properties(key).forEachRemaining(_.remove())
    element match {
      case vertex: Vertex => values.map(reverse).foreach(vertex.property(Cardinality.list, key, _))
      case _              => throw InternalError("Edge doesn't support multi-valued properties")
    }
  }

  override def wrap(us: Seq[D]): Seq[D] = us

  override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
      traversal: Traversal[TD, TG, TC],
      key: String,
      value: Seq[D]
  ): Traversal[TD, TG, TC] = {
    val label = UUID.randomUUID().toString
    traversal.onDeepRaw { gt =>
      value
        .foldLeft(gt.as(label).properties(key).drop().select(label))((t, v) => t.property(Cardinality.list, key, reverse(v)))
        .iterate()
        .asInstanceOf[gt.type]
    }
  }
}

case class SetMapping[D: ClassTag: Renderer, G: ClassTag](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Set[D], D, G](toGraph, toDomain)(implicitly[ClassTag[D]], implicitly[ClassTag[G]], renderer.set, renderer) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.set
  override def readonly: SetMapping[D, G] =
    SetMapping(toGraph, toDomain, isReadonly = true)(implicitly[ClassTag[D]], renderer, implicitly[ClassTag[G]], renderer)
  /*
  Error:(266, 53) not enough arguments for method apply:
  (implicit evidence$9: scala.reflect.ClassTag[D], implicit evidence$10: org.thp.scalligraph.controllers.Renderer[D], implicit evidence$11: scala.reflect.ClassTag[G], implicit renderer: org.thp.scalligraph.controllers.Renderer[D])org.thp.scalligraph.models.SetMapping[D,G] in object SetMapping.
Unspecified value parameter renderer.
    SetMapping(toGraph, toDomain, isReadonly = true)(renderer, implicitly[ClassTag[D]], implicitly[ClassTag[G]])
   */

  //  override def toOutput(d: Set[D]): Output[Set[D]] = Output(d, JsArray(d.map(implicitly[Renderer[D]].toJson(_)).toSeq))

  override def getProperty(element: Element, key: String): Set[D] =
    element
      .properties[G](key)
      .asScala
      .map(p => apply(p.value()))
      .toSet

  override def setProperty(element: Element, key: String, values: Set[D]): Unit = {
    element.properties(key).forEachRemaining(_.remove())
    element match {
      case vertex: Vertex => values.map(reverse).foreach(vertex.property(Cardinality.set, key, _))
      case _              => throw InternalError("Edge doesn't support multi-valued properties")
    }
  }

  override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
      traversal: Traversal[TD, TG, TC],
      key: String,
      value: Set[D]
  ): Traversal[TD, TG, TC] = {
    val label = UUID.randomUUID().toString
    traversal.onDeepRaw { gt =>
      value
        .foldLeft(gt.as(label).properties(key).drop().iterate().select(label))((t, v) => t.property(Cardinality.set, key, reverse(v)))
        .iterate()
        .asInstanceOf[gt.type]
    }
  }

  override def wrap(us: Seq[D]): Set[D] = us.toSet
}
