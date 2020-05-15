package org.thp.scalligraph.controllers

import org.thp.scalligraph.steps.PagedResult
import play.api.libs.json._

trait Renderer[M] {
  type D
  def toOutput(m: M): Output[D]
  def toValue(m: M): D      = toOutput(m).toValue
  def toJson(m: M): JsValue = toOutput(m).toJson
  val isStreamable: Boolean
}

object Renderer {
  type Aux[M, DD] = Renderer[M] { type D = DD }

  def json[M, DD: Writes](f: M => DD): Renderer.Aux[M, DD] = new Renderer[M] {
    override type D = DD
    override def toOutput(m: M): Output[D] = Output(f(m))
    override val isStreamable: Boolean     = false
  }

  def stream[M](f: M => PagedResult[M]): Renderer.Aux[M, List[M]] = new Renderer[M] {
    override type D = List[M]
    override def toOutput(m: M): Output[List[M]] = f(m)
    override val isStreamable: Boolean           = true
  }

  def apply[M](f: M => Output[M]): Renderer.Aux[M, M] = new Renderer[M] {
    override type D = M
    override def toOutput(m: M): Output[M] = f(m)
    override val isStreamable: Boolean     = false
  }

  implicit def seqRenderer[M](implicit aRenderer: Renderer[M]): Renderer.Aux[Seq[M], Seq[aRenderer.D]] = new Renderer[Seq[M]] {
    override type D = Seq[aRenderer.D]
    override def toOutput(m: Seq[M]): Output[D] = {
      val o = m.map(aRenderer.toOutput(_).toValue)
      val j = JsArray(m.map(aRenderer.toOutput(_).toJson))
      Output[D](o, j)
    }
    override val isStreamable: Boolean = false
  }

  implicit def listRenderer[M](implicit aRenderer: Renderer[M]): Renderer.Aux[List[M], List[aRenderer.D]] = new Renderer[List[M]] {
    override type D = List[aRenderer.D]
    override def toOutput(m: List[M]): Output[D] = {
      val o = m.map(aRenderer.toOutput(_).toValue)
      val j = JsArray(m.map(aRenderer.toOutput(_).toJson))
      Output[D](o, j)
    }
    override val isStreamable: Boolean = false
  }

  implicit def setRenderer[M](implicit aRenderer: Renderer[M]): Renderer.Aux[Set[M], Set[aRenderer.D]] = new Renderer[Set[M]] {
    override type D = Set[aRenderer.D]
    override def toOutput(m: Set[M]): Output[D] = {
      val o = m.map(aRenderer.toOutput(_).toValue)
      val j = JsArray(m.map(aRenderer.toOutput(_).toJson).toSeq)
      Output[D](o, j)
    }
    override val isStreamable: Boolean = false
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
