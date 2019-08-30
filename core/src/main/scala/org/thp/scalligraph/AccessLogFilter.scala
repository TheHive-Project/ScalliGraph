package org.thp.scalligraph

import scala.concurrent.ExecutionContext

import play.api.Logger
import play.api.http.{DefaultHttpFilters, EnabledFilters}
import play.api.mvc._

import akka.stream.Materializer
import javax.inject.Inject

class AccessLogFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext) extends EssentialFilter {

  val logger = Logger(getClass)

  override def apply(next: EssentialAction): EssentialAction = (requestHeader: RequestHeader) => {
    val startTime = System.currentTimeMillis
    DiagnosticContext
      .withRequest(requestHeader)(next(requestHeader))
      .map { result =>
        DiagnosticContext.withRequest(requestHeader) {
          val endTime     = System.currentTimeMillis
          val requestTime = endTime - startTime

          logger.info(
            s"${requestHeader.connection.remoteAddressString} ${requestHeader.method} ${requestHeader.uri} took ${requestTime}ms and returned ${result.header.status} ${result
              .body
              .contentLength
              .fold("")(_ + " bytes")}"
          )

          result.withHeaders("Request-Time" -> requestTime.toString)
        }
      }
  }
}

class Filters @Inject()(enabledFilters: EnabledFilters, accessLogFilter: AccessLogFilter)
    extends DefaultHttpFilters(enabledFilters.filters :+ accessLogFilter: _*)
