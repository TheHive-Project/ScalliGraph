package org.thp.scalligraph.models

import org.thp.scalligraph.macros.{JsonMacro, ModelMacro}
import org.thp.scalligraph.services.AttachmentSrv
import play.api.libs.json.Writes

import scala.concurrent.Future
import scala.language.experimental.macros

trait TestUtils {
  def getJsonWrites[T]: Writes[T] = macro JsonMacro.getJsonWrites[T]
  def mkAttachSaver[T]: AttachmentSrv ⇒ T ⇒ Future[T] = macro ModelMacro.mkAttachmentSaver[T]
}
