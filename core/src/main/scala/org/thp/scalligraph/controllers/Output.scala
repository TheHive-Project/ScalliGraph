package org.thp.scalligraph.controllers

import play.api.libs.json._

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
