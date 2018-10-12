package org.thp.scalligraph.models

import org.thp.scalligraph.macros.JsonMacro
import play.api.libs.json.Writes

import scala.language.experimental.macros

object JsonWrites {
  def apply[T]: Writes[T] = macro JsonMacro.getJsonWrites[T]
}
