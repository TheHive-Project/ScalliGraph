package org.thp.scalligraph.controllers

import play.api.libs.json._

class Output[O](_toOutput: => O, _toJson: => JsValue) {
  type OUT = O
  lazy val toOutput: O     = _toOutput
  lazy val toJson: JsValue = _toJson
}

object Output {

  val valWrites: Writes[AnyVal] = Writes[AnyVal] {
    case d: Double  => JsNumber(d)
    case f: Float   => JsNumber(f.toDouble)
    case l: Long    => JsNumber(l)
    case i: Int     => JsNumber(i)
    case c: Char    => JsString(c.toString)
    case s: Short   => JsNumber(s.toInt)
    case b: Byte    => JsNumber(b.toInt)
    case b: Boolean => JsBoolean(b)
//    case _: Unit    ⇒ JsNull
    case _ => JsNull
  }
  def apply[O](native: => O, json: => JsValue): Output[O] = new Output[O](native, json)

  def apply[O](native: => O)(implicit writes: Writes[O]): Output[O] = {
    lazy val n = native
    new Output[O](n, writes.writes(n))
  }
}
