package org.thp.scalligraph

import play.api.libs.json.JsObject

case class BadRequestError(message: String)                                           extends Exception(message)
case class CreateError(status: Option[String], message: String, attributes: JsObject) extends Exception(message)
case class NotFoundError(message: String)                                             extends Exception(message)
case class GetError(message: String)                                                  extends Exception(message)
case class UpdateError(status: Option[String], message: String, attributes: JsObject) extends Exception(message)
case class InternalError(message: String, cause: Throwable = null)                    extends Exception(message, cause)
case class SearchError(message: String, cause: Throwable)                             extends Exception(message, cause)
case class AuthenticationError(message: String)                                       extends Exception(message)
case class AuthorizationError(message: String)                                        extends Exception(message)
case class OAuth2Redirect(redirectUrl: String, params: Map[String, Seq[String]])      extends Exception(redirectUrl)
case class MultiError(message: String, exceptions: Seq[Exception])
    extends Exception(message + exceptions.map(_.getMessage).mkString(" :\n\t- ", "\n\t- ", ""))
