package org.thp.scalligraph

import scala.concurrent.Future

import play.api.Logger
import play.api.http.Status.{BAD_REQUEST, FORBIDDEN, NOT_FOUND}
import play.api.http.{HttpErrorHandler, Status, Writeable}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{RequestHeader, ResponseHeader, Result}

/**
  * This class handles errors. It traverses all causes of exception to find known error and shows the appropriate message
  */
class ErrorHandler extends HttpErrorHandler {
  lazy val logger = Logger(getClass)
  def onClientError(request: RequestHeader, statusCode: Int, message: String): Future[Result] = {
    val tpe = statusCode match {
      case BAD_REQUEST ⇒ "BadRequest"
      case FORBIDDEN   ⇒ "Forbidden"
      case NOT_FOUND   ⇒ "NotFound"
      case _           ⇒ "Unknown"
    }
    Future.successful(toResult(statusCode, Json.obj("type" → tpe, "message" → message)))
  }

  def toErrorResult(ex: Throwable): Option[(Int, JsValue)] =
    ex match {
      case AuthenticationError(message) ⇒
        Some(Status.UNAUTHORIZED → Json.obj("type" → "AuthenticationError", "message" → message))
      case AuthorizationError(message) ⇒
        Some(Status.FORBIDDEN → Json.obj("type" → "AuthorizationError", "message" → message))
      case UpdateError(_, message, attributes) ⇒
        Some(
          Status.INTERNAL_SERVER_ERROR → Json
            .obj("type" → "UpdateError", "message" → message, "object" → attributes))
      case nfe: NumberFormatException ⇒
        Some(
          Status.BAD_REQUEST → Json
            .obj("type" → "NumberFormatException", "message" → ("Invalid format " + nfe.getMessage)))
      case NotFoundError(message) ⇒
        Some(Status.NOT_FOUND → Json.obj("type" → "NotFoundError", "message" → message))
      case BadRequestError(message) ⇒
        Some(Status.BAD_REQUEST → Json.obj("type" → "BadRequest", "message" → message))
      case SearchError(message, cause) ⇒
        Some(
          Status.BAD_REQUEST → Json
            .obj("type" → "SearchError", "message" → s"$message (${cause.getMessage})"))
      case ace: AttributeCheckingError ⇒
        Some(Status.BAD_REQUEST → Json.toJson(ace))
      case iae: IllegalArgumentException ⇒
        Some(Status.BAD_REQUEST → Json.obj("type" → "IllegalArgument", "message" → iae.getMessage))
      case CreateError(_, message, attributes) ⇒
        Some(
          Status.INTERNAL_SERVER_ERROR → Json
            .obj("type" → "CreateError", "message" → message, "object" → attributes))
      case GetError(message) ⇒
        Some(Status.INTERNAL_SERVER_ERROR → Json.obj("type" → "GetError", "message" → message))
      case MultiError(message, exceptions) ⇒
        val suberrors = exceptions.map(e ⇒ toErrorResult(e)).collect {
          case Some((_, j)) ⇒ j
        }
        Some(
          Status.MULTI_STATUS → Json
            .obj("type" → "MultiError", "error" → message, "suberrors" → suberrors))
      case t: Throwable ⇒ Option(t.getCause).flatMap(toErrorResult)
    }

  def toResult[C](status: Int, c: C)(implicit writeable: Writeable[C]) =
    Result(header = ResponseHeader(status), body = writeable.toEntity(c))

  def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {
    val (status, body) = toErrorResult(exception)
      .map { e ⇒
        logger.info("Error", exception)
        e
      }
      .getOrElse {
        logger.error("Internal error", exception)
        Status.INTERNAL_SERVER_ERROR → Json
          .obj("type" → exception.getClass.getName, "message" → exception.getMessage)
      }
    exception match {
      case AuthenticationError(message) ⇒
        Logger.info(s"${request.method} ${request.uri} returned $status: $message")
      case ex ⇒
        Logger.info(s"${request.method} ${request.uri} returned $status", ex)
    }
    Future.successful(toResult(status, body))
  }
}
