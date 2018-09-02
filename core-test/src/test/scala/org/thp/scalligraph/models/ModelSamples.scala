package org.thp.scalligraph.models

import org.scalactic.Good
import org.thp.scalligraph.controllers.{Attachment, FString, FieldsParser}
import org.thp.scalligraph.services.FAttachment
import play.api.libs.json.{JsNull, JsString, Reads, Writes}

object ModelSamples {
  val hobbiesParser: FieldsParser[Seq[String]] = FieldsParser("hobbies") {
    case (_, FString(s)) ⇒ Good(s.split(",").toSeq)
  }
  val hobbiesDatabaseReads: Reads[Seq[String]]   = Reads[Seq[String]](json ⇒ json.validate[String].map(_.split(",").toSeq))
  val hobbiesDatabaseWrites: Writes[Seq[String]] = Writes[Seq[String]](h ⇒ JsString(h.mkString(",")))
  //val hobbiesDatabaseFormat: Format[Seq[String]] = Format[Seq[String]](hobbiesDatabaseReads, hobbiesDatabaseWrites)
  val certificateOutput: Writes[Option[Attachment]] = Writes[Option[Attachment]] {
    case Some(FAttachment(name, _, _, _, id)) ⇒ JsString(s"Certificate $name with id $id")
    case None                                 ⇒ JsNull
    case a                                    ⇒ sys.error(s"Try to output an attachment which is not an AttachmentInputValue (${a.getClass}). Should never happen")
  }
}
