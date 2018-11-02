package org.thp.scalligraph

import play.api.libs.json.{JsObject, OWrites}

class Output[O: OWrites](val toOutput: O) {
  type OUT = O
  def toJson: JsObject = implicitly[OWrites[O]].writes(toOutput)
}
