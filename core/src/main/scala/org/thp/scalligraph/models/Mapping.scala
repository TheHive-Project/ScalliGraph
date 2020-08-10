package org.thp.scalligraph.models

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import java.util.{Base64, Date}

import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{Element, Vertex}
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.Permission
import org.thp.scalligraph.controllers.Renderer
import org.thp.scalligraph.macros.MappingMacro
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{BiConverter, Converter, IdentityConverter, Traversal}
import org.thp.scalligraph.utils.Hash
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{classTag, ClassTag}

object MappingCardinality extends Enumeration {
  val option, single, list, set = Value

  def isCompatible(c1: Value, c2: Value): Boolean =
    c1 == c2 ||
      (c1 == single && c2 == option) ||
      (c1 == option && c2 == single)
}

trait UMapping[D] {
  _: Mapping[D, _, _] =>
  type SingleType
  type GraphType
  def toMapping: Mapping[D, _, _] = this
}

trait MappingLowestPrio {
  implicit def build[T]: UMapping[T] = macro MappingMacro.getOrBuildMapping[T]
}

trait MappingLowerPrio extends MappingLowestPrio {
  implicit val json: SingleMapping[JsValue, String] =
    SingleMapping[JsValue, String](toGraph = j => j.toString, toDomain = s => Json.parse(s))
}

object UMapping extends MappingLowerPrio {
  implicit val permissionWrites: Writes[Permission] = Writes.stringableWrites(Predef.identity)
  implicit val jsObject: SingleMapping[JsObject, String] =
    SingleMapping[JsObject, String](toGraph = j => j.toString, toDomain = s => Json.parse(s).as[JsObject])
  def identity[T: ClassTag: Renderer: NoValue]: IdentityMapping[T] = IdentityMapping[T]()
  val id: SingleMapping[String, AnyRef]                            = SingleMapping[String, AnyRef](toGraph = i => Some(i), toDomain = _.toString)
  implicit val string: IdentityMapping[String]                     = IdentityMapping[String]()
  implicit val long: SingleMapping[Long, JLong]                    = SingleMapping[Long, JLong](toGraph = Long.box, toDomain = Long.unbox)
  implicit val int: SingleMapping[Int, JInt]                       = SingleMapping[Int, JInt](toGraph = Int.box, toDomain = Int.unbox)
  implicit val date: IdentityMapping[Date]                         = IdentityMapping[Date]()
  implicit val boolean: SingleMapping[Boolean, JBoolean]           = SingleMapping[Boolean, JBoolean](toGraph = Boolean.box, toDomain = Boolean.unbox)
  implicit val double: SingleMapping[Double, JDouble]              = SingleMapping[Double, JDouble](toGraph = Double.box, toDomain = Double.unbox)
  implicit val float: SingleMapping[Float, JFloat]                 = SingleMapping[Float, JFloat](toGraph = Float.box, toDomain = Float.unbox)
  implicit val permission: SingleMapping[Permission, String]       = SingleMapping[Permission, String](toGraph = _.toString, toDomain = Permission.apply)
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
  implicit def enum[E <: Enumeration: ClassTag]: SingleMapping[E#Value, String] =
    SingleMapping[E#Value, String](
      _.toString, { s =>
        val rm: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
        val m             = rm.classSymbol(classTag[E].runtimeClass)
        val c             = m.module
        val mod           = c.asModule
        val ref           = rm.reflectModule(mod)
        val i             = ref.instance
        i.asInstanceOf[E].withName(s)
      }
    )

  def jsonNative: SingleMapping[JsValue, Any] = {
    implicit val noValue: NoValue[Any] = NoValue[Any]("")
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

}

abstract class Mapping[M, D: ClassTag, G: ClassTag: NoValue](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D]
)(implicit val getRenderer: Renderer[M], val selectRenderer: Renderer[D])
    extends UMapping[M]
    with BiConverter[D, G] {
  override type SingleType = D
  override type GraphType  = G
  val graphTypeClass: Class[_]  = Option(classTag[G]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val domainTypeClass: Class[_] = Option(classTag[D]).fold[Class[_]](classOf[Any])(c => convertToJava(c.runtimeClass))
  val cardinality: MappingCardinality.Value
  val noValue: G = implicitly[NoValue[G]].apply()
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
}

case class IdentityMapping[T: ClassTag: NoValue: Renderer](override val isReadonly: Boolean = false)
    extends SingleMapping[T, T](identity[T], identity[T], isReadonly)
    with IdentityConverter[T] {}

class SingleMapping[D: ClassTag, G: ClassTag: NoValue](
    toGraph: D => G,
    toDomain: G => D,
    override val isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[D, D, G](toGraph, toDomain) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
  override def readonly: SingleMapping[D, G]         = new SingleMapping[D, G](toGraph, toDomain, isReadonly = true)
  override def optional: OptionMapping[D, G]         = OptionMapping[D, G](toGraph, toDomain, isReadonly)
  override def sequence: ListMapping[D, G]           = ListMapping[D, G](toGraph, toDomain, isReadonly)
  override def set: SetMapping[D, G]                 = SetMapping[D, G](toGraph, toDomain, isReadonly)

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
    traversal.onRaw(_.property(key, reverse(value)))

  override def wrap(us: Seq[D]): D = us.head
}

object SingleMapping {
  def apply[D: ClassTag, G: ClassTag: NoValue](toGraph: D => G, toDomain: G => D, isReadonly: Boolean = false)(implicit renderer: Renderer[D]) =
    new SingleMapping[D, G](toGraph, toDomain, isReadonly)
}

object IdMapping extends Mapping[String, String, AnyRef] {
  override val cardinality: MappingCardinality.Value     = MappingCardinality.single
  override def apply(g: AnyRef): String                  = g.toString
  override val isReadonly: Boolean                       = true
  override def readonly: Mapping[String, String, AnyRef] = this

  override val reverse: Converter[AnyRef, String] = (v: String) => v

  override def getProperty(element: Element, key: String): String = element.id().toString

  override def setProperty(element: Element, key: String, value: String): Unit = throw InternalError("Property ID is readonly")

  override def wrap(us: Seq[String]): String = us.head

  override def setProperty[TD, TG <: Element, TC <: Converter[TD, TG]](
      traversal: Traversal[TD, TG, TC],
      key: String,
      value: String
  ): Traversal[TD, TG, TC] = throw InternalError("Id attribute is read-only")
}

case class OptionMapping[D: ClassTag, G: ClassTag: NoValue](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Option[D], D, G](toGraph, toDomain)(
      implicitly[ClassTag[D]],
      implicitly[ClassTag[G]],
      implicitly[NoValue[G]],
      renderer.opt,
      renderer
    ) { thisMapping =>
  override val cardinality: MappingCardinality.Value = MappingCardinality.option
  override def readonly: OptionMapping[D, G]         = copy(isReadonly = true)
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
    value.fold(traversal.removeProperty(key))(v => traversal.onRaw(_.property(key, reverse(v))))

  override def wrap(us: Seq[D]): Option[D] = us.headOption
}

case class ListMapping[D: ClassTag, G: ClassTag: NoValue](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Seq[D], D, G](toGraph, toDomain)(
      implicitly[ClassTag[D]],
      implicitly[ClassTag[G]],
      implicitly[NoValue[G]],
      renderer.list,
      renderer
    ) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.list
  override def readonly: ListMapping[D, G]           = copy(isReadonly = true)

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
  ): Traversal[TD, TG, TC] =
    value.foldLeft(traversal.removeProperty(key))((t, v) => t.onRaw(_.property(Cardinality.list, key, reverse(v))))
}

case class SetMapping[D: ClassTag: Renderer, G: ClassTag: NoValue](
    toGraph: D => G = (_: D).asInstanceOf[G],
    toDomain: G => D = (_: G).asInstanceOf[D],
    isReadonly: Boolean = false
)(implicit renderer: Renderer[D])
    extends Mapping[Set[D], D, G](toGraph, toDomain)(implicitly[ClassTag[D]], implicitly[ClassTag[G]], implicitly[NoValue[G]], renderer.set, renderer) {
  override val cardinality: MappingCardinality.Value = MappingCardinality.set
  override def readonly: SetMapping[D, G] =
    SetMapping(toGraph, toDomain, isReadonly = true)(implicitly[ClassTag[D]], renderer, implicitly[ClassTag[G]], implicitly[NoValue[G]], renderer)

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
  ): Traversal[TD, TG, TC] = value.foldLeft(traversal.removeProperty(key))((t, v) => t.onRaw(_.property(Cardinality.set, key, reverse(v))))

  override def wrap(us: Seq[D]): Set[D] = us.toSet
}
