package org.thp.scalligraph

import play.api.libs.json._

class Output[O](_toOutput: ⇒ O, _toJson: ⇒ JsValue) {
  type OUT = O
  lazy val toOutput: O     = _toOutput
  lazy val toJson: JsValue = _toJson
}
//class Output[O: Writes](val toOutput: O) {
//  type OUT = O
//  def toJson: JsValue = implicitly[Writes[O]].writes(toOutput)
//}

object Output {
  val valWrites: Writes[AnyVal] = Writes[AnyVal] {
    case d: Double  ⇒ JsNumber(d)
    case f: Float   ⇒ JsNumber(f.toDouble)
    case l: Long    ⇒ JsNumber(l)
    case i: Int     ⇒ JsNumber(i)
    case c: Char    ⇒ JsString(c.toString)
    case s: Short   ⇒ JsNumber(s.toInt)
    case b: Byte    ⇒ JsNumber(b.toInt)
    case b: Boolean ⇒ JsBoolean(b)
//    case _: Unit    ⇒ JsNull
    case _ ⇒ JsNull
  }
  def apply[O](native: ⇒ O, json: ⇒ JsValue): Output[O]            = new Output[O](native, json)
  def apply[O](native: ⇒ O)(implicit writes: Writes[O]): Output[O] = new Output[O](native, writes.writes(native)) // FIXME cache native
}
