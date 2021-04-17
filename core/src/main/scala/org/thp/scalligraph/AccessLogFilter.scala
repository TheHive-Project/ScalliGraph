package org.thp.scalligraph

import play.api.Logger
import play.api.mvc._

import scala.concurrent.ExecutionContext

class AccessLogFilter(implicit ec: ExecutionContext) extends EssentialFilter {

  val logger: Logger = Logger(getClass)

  override def apply(next: EssentialAction): EssentialAction =
    (requestHeader: RequestHeader) => {
      val startTime = System.currentTimeMillis
      DiagnosticContext
        .withRequest(requestHeader)(next(requestHeader))
        .recoverWith { case error => ErrorHandler.onServerError(requestHeader, error) }
        .map { result =>
          DiagnosticContext.withRequest(requestHeader) {
            val endTime     = System.currentTimeMillis
            val requestTime = endTime - startTime

            logger.info(
              s"${requestHeader.remoteAddress} ${requestHeader.method} ${requestHeader.uri} took ${requestTime}ms and returned ${result.header.status} ${result
                .body
                .contentLength
                .fold("")(_ + " bytes")}"
            )

            result.withHeaders("Request-Time" -> requestTime.toString)
          }
        }
    }
}
