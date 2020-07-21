package org.thp.scalligraph.controllers

import org.thp.scalligraph.steps.IteratorOutput
import play.api.libs.json._

trait Renderer[V] { renderer =>
  def toOutput(value: V): Output[V]
  def toJson(value: V): JsValue = toOutput(value).toJson

  def opt: Renderer[Option[V]] = new Renderer[Option[V]] {
    override def toOutput(value: Option[V]): Output[Option[V]] = new Output[Option[V]] {
      override def toValue: Option[V] = value
      override def toJson: JsValue    = value.fold[JsValue](JsNull)(renderer.toJson)
    }
  }

  def list: Renderer[Seq[V]] = new Renderer[Seq[V]] {
    override def toOutput(value: Seq[V]): Output[Seq[V]] = new Output[Seq[V]] {
      override def toValue: Seq[V] = value
      override def toJson: JsValue = JsArray(value.map(renderer.toJson))
    }
  }

  def set: Renderer[Set[V]] = new Renderer[Set[V]] {
    override def toOutput(value: Set[V]): Output[Set[V]] = new Output[Set[V]] {
      override def toValue: Set[V] = value
      override def toJson: JsValue = JsArray(value.map(renderer.toJson).toSeq)
    }
  }
}

trait RendererLowPriority {
  implicit def json[V: Writes]: Renderer[V] = new Renderer[V] {
    override def toOutput(value: V): Output[V] = Output(value)
  }
}

class StreamRenderer[F, V](f: F => IteratorOutput[V])(implicit renderer: Renderer[V]) extends Renderer[F] {
  override def toOutput(value: F): Output[F] = new Output[F] {
    override def toValue: F      = value
    override def toJson: JsValue = JsArray(f(value).iterator.map(renderer.toJson).toSeq)
  }
  def toStream(value: F): (Iterator[JsValue], Option[Long]) = {
    val iteratorOutput = f(value)
    iteratorOutput.iterator.map(renderer.toJson) -> iteratorOutput.totalSize.map(_.apply())
  }
}

object Renderer extends RendererLowPriority {
  implicit def jsValue[J <: JsValue]: Renderer[J] = (m: J) => Output(m)

  def toJson[F, V: Writes](f: F => V): Renderer[F] = (value: F) => Output(value, Json.toJson(f(value)))

  def stream[F, V](f: F => IteratorOutput[V])(implicit renderer: Renderer[V]): Renderer[F] = new StreamRenderer[F, V](f)

  def apply[V](f: V => Output[V]): Renderer[V] = (m: V) => f(m)

  implicit def seqRenderer[V](implicit aRenderer: Renderer[V]): Renderer[Seq[V]] = new Renderer[Seq[V]] {
    override def toOutput(value: Seq[V]): Output[Seq[V]] =
      Output[Seq[V]](value.map(aRenderer.toOutput(_).toValue), JsArray(value.map(aRenderer.toOutput(_).toJson)))
  }

//  implicit def listRenderer[M](implicit aRenderer: Renderer[M]): Renderer[List[M], List[aRenderer.D]] = new Renderer[List[M]] {
//    override def toOutput(m: List[M]): Output[D] = {
//      val o = m.map(aRenderer.toOutput(_).toValue)
//      val j = JsArray(m.map(aRenderer.toOutput(_).toJson))
//      Output[D](o, j)
//    }
//  }

  implicit def setRenderer[V](implicit aRenderer: Renderer[V]): Renderer[Set[V]] = new Renderer[Set[V]] {
    override def toOutput(value: Set[V]): Output[Set[V]] =
      Output[Set[V]](value.map(aRenderer.toOutput(_).toValue), JsArray(value.map(aRenderer.toOutput(_).toJson).toSeq))
  }
}

trait Output[+O] {
  def toValue: O
  def toJson: JsValue
}

class SimpleOutput[O](getValue: () => O, getJson: () => JsValue) extends Output[O] {
  lazy val toValue: O      = getValue()
  lazy val toJson: JsValue = getJson()
}

object Output {
  def apply[O](native: => O, json: => JsValue): Output[O] = new SimpleOutput[O](() => native, () => json)

  def apply[O](native: => O)(implicit writes: Writes[O]): Output[O] = {
    lazy val n = native
    new SimpleOutput[O](() => n, () => writes.writes(n))
  }
}
