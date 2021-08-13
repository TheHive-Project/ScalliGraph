package org.thp.scalligraph.controllers

import org.thp.scalligraph.traversal.IteratorOutput
import play.api.libs.json._

trait Renderer[V] { renderer =>
  type O
  def toValue(value: V): O
  def toOutput(value: V): Output
  def toJson(value: V): JsValue = toOutput(value).content

  def opt: Renderer.Aux[Option[V], Option[O]] =
    new Renderer[Option[V]] {
      override type O = Option[renderer.O]
      override def toValue(value: Option[V]): O       = value.map(renderer.toValue)
      override def toOutput(value: Option[V]): Output = Output(value.fold[JsValue](JsNull)(renderer.toJson))
    }

  def list: Renderer.Aux[Seq[V], Seq[O]] =
    new Renderer[Seq[V]] {
      override type O = Seq[renderer.O]
      override def toValue(value: Seq[V]): O       = value.map(renderer.toValue)
      override def toOutput(value: Seq[V]): Output = Output(JsArray(value.map(renderer.toJson)))
    }

  def set: Renderer.Aux[Set[V], Set[O]] =
    new Renderer[Set[V]] {
      override type O = Set[renderer.O]
      override def toValue(value: Set[V]): O       = value.map(renderer.toValue)
      override def toOutput(value: Set[V]): Output = Output(JsArray(value.map(renderer.toJson).toSeq))
    }
}

class StreamRenderer[F](f: F => IteratorOutput) extends Renderer[F] {
  type O = JsValue
  override def toOutput(value: F): IteratorOutput = f(value)
  override def toValue(value: F): JsValue         = f(value).content
}

trait RendererLowPriority {
  implicit def json[V: Writes]: Renderer.Aux[V, V] =
    new Renderer[V] {
      override type O = V
      override def toValue(value: V): O       = value
      override def toOutput(value: V): Output = Output(Json.toJson(value))
    }
}

object Renderer extends RendererLowPriority {
  type Aux[V, OO] = Renderer[V] { type O = OO }

  implicit def jsValue[V <: JsValue]: Renderer.Aux[V, V] =
    new Renderer[V] {
      override type O = V
      override def toValue(value: V): O       = value
      override def toOutput(value: V): Output = Output(Json.toJson(value))
    }

  def toJson[F, V: Writes](f: F => V): Renderer.Aux[F, V] =
    new Renderer[F] {
      override type O = V
      override def toValue(value: F): V       = f(value)
      override def toOutput(value: F): Output = Output(Json.toJson(f(value)))
    }

  def stream[F](f: F => IteratorOutput): StreamRenderer[F] = new StreamRenderer[F](f)

  def apply[V](f: V => Output): Renderer.Aux[V, JsValue] =
    new Renderer[V] {
      override type O = JsValue
      override def toValue(value: V): JsValue = f(value).content
      override def toOutput(value: V): Output = f(value)
    }

  implicit def seqRenderer[F](implicit aRenderer: Renderer[F]): Renderer.Aux[Seq[F], Seq[aRenderer.O]] = aRenderer.list
}

trait Output {
  def content: JsValue
}

object Output {
  def apply[T: Writes](content: T): Output = new SimpleOutput(Json.toJson(content))
}

class SimpleOutput(val content: JsValue) extends Output
