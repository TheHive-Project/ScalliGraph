package org.thp.scalligraph.steps

import java.util.{List => JList, Map => JMap}

import org.thp.scalligraph.controllers.{Output, Renderer}
import org.thp.scalligraph.models._
import org.thp.scalligraph.steps.StepsOps._
import play.api.libs.json.{JsArray, JsValue}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class PagedResult[R: Renderer](result: Traversal[R, G, Converter[R, G]] forSome { type G }, totalSize: Option[Traversal[Long, _, _]])
    extends Output[List[R]] {
  override lazy val toJson: JsValue                         = JsArray(result.cast[R, Any].toSeq.map(implicitly[Renderer[R]].toOutput(_).toJson))
  override lazy val toValue: List[R]                        = result.cast[R, Any].toList
  val subRenderer: Renderer[R]                              = implicitly[Renderer[R]]
  def map[T: Renderer: ClassTag](f: R => T): PagedResult[T] = PagedResult(result.map(f), totalSize)
  def toSource: (Iterator[JsValue], Option[Long])           = result.cast[R, Any].toIterator.map(subRenderer.toJson) -> totalSize.map(_.cast[Long, Any].head())
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

// FIXME
import org.thp.scalligraph.models.UniMapping.fakeRenderer
object IdMapping extends Mapping[String, String, AnyRef] {
  override val cardinality: MappingCardinality.Value     = MappingCardinality.single
  override def toGraphOpt(d: String): Option[AnyRef]     = Some(d)
  override def apply(g: AnyRef): String                  = g.toString
  override val isReadonly: Boolean                       = true
  override def readonly: Mapping[String, String, AnyRef] = this

  override def reverse: Converter[AnyRef, String] = (v: String) => v
}
