package org.thp.scalligraph

import play.api.libs.json.{JsValue, Json, Writes}

class Public[A: Writes] { _: A ⇒
  def toJson: JsValue = Json.toJson(this)
}
