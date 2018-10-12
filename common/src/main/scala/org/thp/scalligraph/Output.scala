package org.thp.scalligraph

import play.api.libs.json.{JsValue, Json, Writes}

class Output[O: Writes](val toOutput: O) {
  type OUT = O
  def toJson: JsValue = Json.toJson(toOutput)
}
