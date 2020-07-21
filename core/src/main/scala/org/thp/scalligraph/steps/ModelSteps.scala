package org.thp.scalligraph.steps

import java.util.{List => JList, Map => JMap}

import gremlin.scala.Element
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.controllers.{Output, Renderer}
import org.thp.scalligraph.models._
import org.thp.scalligraph.steps.StepsOps._
import play.api.libs.json.{JsArray, JsValue}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class IteratorOutput[V](val iterator: Iterator[V], val totalSize: Option[() => Long]) // FIXME rename to SizedIterator
//                       (implicit renderer: Renderer[V])
//    extends Output[IteratorOutput[V]] {
//  override def toValue: IteratorOutput[V]                      = this
//  override def toJson: JsValue                                 = JsArray(iterator.map(renderer.toJson).toSeq)
//  def map[T: Renderer: ClassTag](f: V => T): IteratorOutput[T] = new IteratorOutput(iterator.map(f), totalSize)
//  def jsonIterator: Iterator[JsValue]                          = iterator.map(renderer.toJson)
////  def toSource: (Iterator[JsValue], Option[Long]) =
////    iterator.map(renderer.toJson) -> totalSize.map(_.apply())
//}

object IteratorOutput {
  def apply[V](traversal: Traversal[V, _, _], totalSize: Option[() => Long] = None) =
    new IteratorOutput[V](traversal.cast[V, Any].toIterator, totalSize)
}

class PagedResult[R: Renderer](traversal: Traversal[R, G, Converter[R, G]] forSome { type G }, totalSize: Option[Traversal[Long, _, _]])
    extends Output[List[R]] {
  override lazy val toJson: JsValue                         = JsArray(traversal.cast[R, Any].toSeq.map(implicitly[Renderer[R]].toOutput(_).toJson))
  override lazy val toValue: List[R]                        = traversal.cast[R, Any].toList
  val subRenderer: Renderer[R]                              = implicitly[Renderer[R]]
  def map[T: Renderer: ClassTag](f: R => T): PagedResult[T] = PagedResult(traversal.map(f), totalSize)
  def toSource: (Iterator[JsValue], Option[Long]) =
    traversal.cast[R, Any].toIterator.map(subRenderer.toJson) -> totalSize.map(_.cast[Long, Any].head())
}
object PagedResult {
//  def apply[D, G](result: Traversal[D, _, _], totalSize: Option[Traversal[Long, _, _]], renderer: Renderer[D]) =
//    new PagedResult[D](result.cast[D, G], totalSize)(renderer)
  def apply[R: Renderer](result: Traversal[R, _, _], totalSize: Option[Traversal[Long, _, _]]) =
    new PagedResult[R](result.cast[R, Any], totalSize)(implicitly[Renderer[R]])
}

class ValueMap(m: Map[String, Seq[AnyRef]]) {
  def get[A](name: String): A = m(name).head.asInstanceOf[A]
}

object ValueMap {

  def unapply(m: JMap[_, _]): Option[ValueMap] =
    Some(new ValueMap(m.asScala.map { case (k, v) => k.toString -> v.asInstanceOf[JList[AnyRef]].asScala }.toMap))
}

//class SelectMap(m: Map[String, Any]) {
//  def get[A](name: String): A        = m(name).asInstanceOf[A]
//  def get[A](label: StepLabel[A]): A = m(label.name).asInstanceOf[A]
//}

//object SelectMap {
//
//  def unapply(m: JMap[_, _]): Option[SelectMap] =
//    Some(new SelectMap(m.asScala.map { case (k, v) => k.toString -> v }.toMap))
//}
//

object IdMapping extends Mapping[String, String, AnyRef] {
  override val cardinality: MappingCardinality.Value = MappingCardinality.single
//  override def toGraphOpt(d: String): Option[AnyRef]     = Some(d)
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
  ): Traversal[TD, TG, TC] = ???
}
