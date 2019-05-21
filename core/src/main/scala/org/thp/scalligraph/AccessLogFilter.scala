package org.thp.scalligraph

import scala.concurrent.{ExecutionContext, Future}

import play.api.Logger
import play.api.http.{DefaultHttpFilters, EnabledFilters}
import play.api.mvc._

import akka.stream.Materializer
import javax.inject.Inject

class AccessLogFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

  def apply(nextFilter: RequestHeader ⇒ Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val startTime = System.currentTimeMillis

    nextFilter(requestHeader).map { result ⇒
      val endTime     = System.currentTimeMillis
      val requestTime = endTime - startTime

      Logger.info(s"${requestHeader.method} ${requestHeader.uri} took ${requestTime}ms and returned ${result.header.status} ${result
        .body
        .contentLength
        .fold("")(_ + " bytes")}")

      result.withHeaders("Request-Time" → requestTime.toString)
    }
  }
}

class Filters @Inject()(enabledFilters: EnabledFilters, accessLogFilter: AccessLogFilter)
    extends DefaultHttpFilters(enabledFilters.filters :+ accessLogFilter: _*)
