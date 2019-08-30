package org.thp.scalligraph

import scala.concurrent.Future

import play.api.Logger
import play.api.http.Status.{BAD_REQUEST, FORBIDDEN, NOT_FOUND}
import play.api.http.{HttpErrorHandler, Status, Writeable}
import play.api.libs.json.{JsString, JsValue, Json}
import play.api.mvc.{RequestHeader, ResponseHeader, Result}

/**
  * This class handles errors. It traverses all causes of exception to find known error and shows the appropriate message
  */
class ErrorHandler extends HttpErrorHandler {
  lazy val logger = Logger(getClass)

  def onClientError(request: RequestHeader, statusCode: Int, message: String): Future[Result] = {
    val tpe = statusCode match {
      case BAD_REQUEST => "BadRequest"
      case FORBIDDEN   => "Forbidden"
      case NOT_FOUND   => "NotFound"
      case _           => "Unknown"
    }
    Future.successful(toResult(statusCode, Json.obj("type" -> tpe, "message" -> message)))
  }

  def toErrorResult(ex: Throwable): Option[(Int, JsValue)] =
    ex match {
      case e: AuthenticationError    => Some(Status.UNAUTHORIZED          -> e.toJson)
      case e: AuthorizationError     => Some(Status.FORBIDDEN             -> e.toJson)
      case e: CreateError            => Some(Status.INTERNAL_SERVER_ERROR -> e.toJson)
      case e: GetError               => Some(Status.INTERNAL_SERVER_ERROR -> e.toJson)
      case e: SearchError            => Some(Status.BAD_REQUEST           -> e.toJson)
      case e: UpdateError            => Some(Status.INTERNAL_SERVER_ERROR -> e.toJson)
      case e: NotFoundError          => Some(Status.NOT_FOUND             -> e.toJson)
      case e: BadRequestError        => Some(Status.BAD_REQUEST           -> e.toJson)
      case e: MultiError             => Some(Status.MULTI_STATUS          -> e.toJson)
      case e: AttributeCheckingError => Some(Status.BAD_REQUEST           -> e.toJson)
      case e: InternalError          => Some(Status.INTERNAL_SERVER_ERROR -> e.toJson)
      case e: BadConfigurationError  => Some(Status.BAD_REQUEST           -> e.toJson)
//      case e: OAuth2Redirect
      case nfe: NumberFormatException =>
        Some(Status.BAD_REQUEST -> Json.obj("type" -> "NumberFormatException", "message" -> ("Invalid format " + nfe.getMessage)))
      case iae: IllegalArgumentException =>
        Some(Status.BAD_REQUEST -> Json.obj("type" -> "IllegalArgument", "message" -> iae.getMessage))
      case t: Throwable => Option(t.getCause).flatMap(toErrorResult)
    }

  def toResult[C](status: Int, c: C)(implicit writeable: Writeable[C]) =
    Result(header = ResponseHeader(status), body = writeable.toEntity(c))

  def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {
    val (status, body) = toErrorResult(exception)
      .getOrElse {
        logger.error("Internal error", exception)
        val json = Json.obj("type" -> exception.getClass.getName, "message" -> exception.getMessage)
        Status.INTERNAL_SERVER_ERROR -> (if (exception.getCause == null) json else json + ("cause" -> JsString(exception.getCause.getMessage)))
      }
    exception match {
      case AuthenticationError(message, _) => logger.warn(s"${request.method} ${request.uri} returned $status: $message")
      case ex                              => logger.warn(s"${request.method} ${request.uri} returned $status", ex)
    }
    Future.successful(toResult(status, body))
  }
}
