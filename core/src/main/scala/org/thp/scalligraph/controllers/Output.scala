package org.thp.scalligraph.controllers

import play.api.libs.json._

trait Outputer[M] {
  type D
  def toOutput(m: M): Output[D]
}

object Outputer {
  type Aux[M, DD] = Outputer[M] { type D = DD }

  def apply[M, DD: Writes](f: M => DD): Outputer.Aux[M, DD] = new Outputer[M] {
    override type D = DD
    override def toOutput(m: M): Output[D] = Output(f(m))
  }
  implicit class OutputOps[O, D](o: O)(implicit outputer: Outputer.Aux[O, D]) {
    def toJson: JsValue = outputer.toOutput(o).toJson
    def toOutput: D     = outputer.toOutput(o).toOutput
  }
}

class Output[O](_toOutput: => O, _toJson: => JsValue) {
  type OUT = O
  lazy val toOutput: O     = _toOutput
  lazy val toJson: JsValue = _toJson
}

object Output {
  def apply[O](native: => O, json: => JsValue): Output[O] = new Output[O](native, json)

  def apply[O](native: => O)(implicit writes: Writes[O]): Output[O] = {
    lazy val n = native
    new Output[O](n, writes.writes(n))
  }
}
