package org.thp.scalligraph

import play.api.libs.json._

import org.thp.scalligraph.controllers.Field

case class BadRequestError(message: String)                                           extends Exception(message)
case class CreateError(status: Option[String], message: String, attributes: JsObject) extends Exception(message)
case class NotFoundError(message: String)                                             extends Exception(message)
case class GetError(message: String)                                                  extends Exception(message)
case class UpdateError(status: Option[String], message: String, attributes: JsObject) extends Exception(message)
case class InternalError(message: String, cause: Throwable = null)                    extends Exception(message, cause)
case class SearchError(message: String, cause: Throwable)                             extends Exception(message, cause)
case class AuthenticationError(message: String)                                       extends Exception(message)
case class AuthorizationError(message: String)                                        extends Exception(message)
case class BadConfigurationError(message: String)                                     extends Exception(message)
case class OAuth2Redirect(redirectUrl: String, params: Map[String, Seq[String]])      extends Exception(redirectUrl)
case class MultiError(message: String, exceptions: Seq[Exception])
    extends Exception(message + exceptions.map(_.getMessage).mkString(" :\n\t- ", "\n\t- ", ""))

case class AttributeCheckingError(errors: Seq[AttributeError] = Nil) extends Exception(errors.mkString("[", "][", "]")) {
  override def toString: String = errors.mkString("[", "][", "]")
}

object AttributeCheckingError {
  implicit val invalidFormatAttributeErrorWrites: OWrites[InvalidFormatAttributeError] =
    Json.writes[InvalidFormatAttributeError]
  implicit val unknownAttributeErrorWrites: OWrites[UnknownAttributeError] = Json.writes[UnknownAttributeError]
  implicit val updateReadOnlyAttributeErrorWrites: OWrites[UpdateReadOnlyAttributeError] =
    Json.writes[UpdateReadOnlyAttributeError]
  implicit val missingAttributeErrorWrites: OWrites[MissingAttributeError] = Json.writes[MissingAttributeError]
  implicit val unsupportedAttributeErrorWrites: OWrites[UnsupportedAttributeError] =
    Json.writes[UnsupportedAttributeError]

  implicit val attributeErrorWrites: Writes[AttributeError] = Writes[AttributeError] {
    case ifae: InvalidFormatAttributeError =>
      invalidFormatAttributeErrorWrites.writes(ifae) + ("type" -> JsString("InvalidFormatAttributeError"))
    case uae: UnknownAttributeError =>
      unknownAttributeErrorWrites.writes(uae) + ("type" -> JsString("UnknownAttributeError"))
    case uroae: UpdateReadOnlyAttributeError =>
      updateReadOnlyAttributeErrorWrites.writes(uroae) + ("type" -> JsString("UpdateReadOnlyAttributeError"))
    case mae: MissingAttributeError =>
      missingAttributeErrorWrites.writes(mae) + ("type" -> JsString("MissingAttributeError"))
    case uae: UnsupportedAttributeError =>
      unsupportedAttributeErrorWrites.writes(uae) + ("type" -> JsString("UnsupportedAttributeError"))
  }

  implicit val attributeCheckingErrorWrites: OWrites[AttributeCheckingError] =
    Json.writes[AttributeCheckingError]
}

sealed trait AttributeError extends Throwable {
  val name: String
  def withName(name: String): AttributeError

  override def getMessage: String = toString
}

case class InvalidFormatAttributeError(name: String, format: String, acceptedInput: Set[String], field: Field) extends AttributeError {
  override def toString = s"Invalid format for $name: $field, expected $format ${acceptedInput.mkString("(", ",", ")")}"
  override def withName(newName: String): InvalidFormatAttributeError =
    copy(name = newName)
}
case class UnknownAttributeError(name: String, field: Field) extends AttributeError {
  override def toString = s"Unknown attribute $name: $field"
  override def withName(newName: String): UnknownAttributeError =
    copy(name = newName)
}
case class UpdateReadOnlyAttributeError(name: String) extends AttributeError {
  override def toString = s"Attribute $name is read-only"
  override def withName(newName: String): UpdateReadOnlyAttributeError =
    copy(name = newName)
}
case class MissingAttributeError(name: String) extends AttributeError {
  override def toString = s"Attribute $name is missing"
  override def withName(newName: String): MissingAttributeError =
    copy(name = newName)
}
case class UnsupportedAttributeError(name: String) extends AttributeError {
  override def toString = s"Attribute $name is not supported"
  override def withName(newName: String): UnsupportedAttributeError =
    copy(name = newName)
}
