package org.thp.scalligraph.steps

import java.util.{List => JList, Map => JMap}

import gremlin.scala._
import gremlin.scala.dsl._
import org.thp.scalligraph.controllers.{Output, Renderer}
import org.thp.scalligraph.models._
import org.thp.scalligraph.steps.StepsOps._
import play.api.libs.json.{JsArray, JsValue}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

case class PagedResult[R: Renderer](result: Traversal[R, _], totalSize: Option[Traversal[Long, _]]) extends Output[List[R]] {
  override lazy val toJson: JsValue                         = JsArray(result.toList.map(implicitly[Renderer[R]].toOutput(_).toJson))
  override lazy val toValue: List[R]                        = result.toList
  val subRenderer: Renderer[R]                              = implicitly[Renderer[R]]
  def map[T: Renderer: ClassTag](f: R => T): PagedResult[T] = PagedResult(result.map(f), totalSize)
}
object PagedResult {
  def apply(result: UntypedTraversal, totalSize: Option[Traversal[Long, _]], renderer: Renderer[Any]) =
    new PagedResult[Any](result.typed[Any, Any], totalSize)(renderer)
}

class ValueMap(m: Map[String, Seq[AnyRef]]) {
  def get[A](name: String): A = m(name).head.asInstanceOf[A]
}

object ValueMap {

  def unapply(m: JMap[_, _]): Option[ValueMap] =
    Some(new ValueMap(m.asScala.map { case (k, v) => k.toString -> v.asInstanceOf[JList[AnyRef]].asScala }.toMap))
}

class SelectMap(m: Map[String, Any]) {
  def get[A](name: String): A        = m(name).asInstanceOf[A]
  def get[A](label: StepLabel[A]): A = m(label.name).asInstanceOf[A]
}

object SelectMap {

  def unapply(m: JMap[_, _]): Option[SelectMap] =
    Some(new SelectMap(m.asScala.map { case (k, v) => k.toString -> v }.toMap))
}

object IdMapping extends Mapping[String, String, AnyRef] {
  override val cardinality: MappingCardinality.Value     = MappingCardinality.single
  override def toGraphOpt(d: String): Option[AnyRef]     = Some(d)
  override def toDomain(g: AnyRef): String               = g.toString
  override val isReadonly: Boolean                       = true
  override def readonly: Mapping[String, String, AnyRef] = this
}
