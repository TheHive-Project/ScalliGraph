package org.thp.scalligraph.controllers

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.{Success, Try}

import play.api.Logger
import play.api.http.HttpErrorHandler
import play.api.libs.json.{Json, Writes}
import play.api.mvc.Results.BadRequest
import play.api.mvc._

import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import javax.inject.{Inject, Singleton}
import org.scalactic.{Bad, Good}
import org.slf4j.MDC
import org.thp.scalligraph.AttributeCheckingError
import org.thp.scalligraph.record.Record
import shapeless.labelled.FieldType
import shapeless.{::, labelled, HList, HNil, Witness}

/**
  * API entry point. This class create a controller action which parse request and check authentication
  *
  * @param authenticateSrv method that check user authentication
  * @param actionBuilder ActionBuilder
  * @param ec            ExecutionContext
  */
@Singleton
class EntryPoint @Inject()(
    authenticateSrv: AuthenticateSrv,
    actionBuilder: DefaultActionBuilder,
    errorHandler: HttpErrorHandler,
    implicit val ec: ExecutionContext,
    implicit val mat: Materializer) {

  lazy val logger = Logger(getClass)

  /**
    * Create a named entry point
    *
    * @param name name of entry point
    * @return empty entry point
    */
  def apply(name: String): EntryPointBuilder[HNil, Request] =
    EntryPointBuilder[HNil, Request](name, BaseFieldsParser.good(HNil), Future.successful)

  /**
    * An entry point is defined by its name, a fields parser which transform request into a record V and the type of request (
    * authenticated or not)
    *
    * @param name         name of the entry point
    * @param fieldsParser fields parser that transform request into a record of type V
    * @param req          request transformer function
    * @tparam V type of record
    * @tparam R type of request (Request of AuthenticatedRequest)
    */
  case class EntryPointBuilder[V <: HList, R[_] <: Request[_]](
      name: String,
      fieldsParser: BaseFieldsParser[V],
      req: Request[AnyContent] ⇒ Future[R[AnyContent]]) {

    /**
      * Extract a field from request.
      *
      * @param fp field parser to use to extract value from request
      * @tparam T type of extracted field
      * @return a new entry point with added fields parser
      */
    def extract[N, T](fieldName: Witness.Aux[N], fp: BaseFieldsParser[T]): EntryPointBuilder[FieldType[N, T] :: V, R] =
      EntryPointBuilder(name, fieldsParser.andThen(fieldName.toString)(fp)(labelled.field[N](_) :: _), req)

    /**
      * Add an authentication check to this entry point.
      *
      * @return a new entry point with added authentication check
      */
    def authenticated: EntryPointBuilder[V, AuthenticatedRequest] =
      EntryPointBuilder[V, AuthenticatedRequest](
        name,
        fieldsParser,
        request ⇒
          Future.fromTry {
            authenticateSrv.getAuthContext(request).flatMap { authContext ⇒
              logger.trace(s"check user permissions of ${authContext.userName}")
              Success(new AuthenticatedRequest[AnyContent](authContext, request))
            }
        }
      )

    /**
      * Materialize action using a function that transform request with parsed record info stream of writable
      *
      * @param block business logic function that transform request into stream of element
      * @tparam T type of element in stream. Element must be writable to JSON
      * @return Action
      */
    def chunked[T: Writes](block: R[Record[V]] ⇒ Source[T, Long]): Action[AnyContent] = {
      def sourceToResult(src: Source[T, Long]): Result = {

        val (numberOfElement, publisher) = src
          .map(t ⇒ Json.toJson(t).toString)
          .intersperse("[", ",", "]")
          .toMat(Sink.asPublisher(false))(Keep.both)
          .run()

        Results.Ok
          .chunked {
            Source.fromPublisher(publisher)
          }
          .as("application/json")
          .withHeaders("X-total" → numberOfElement.toString)
      }

      actionBuilder.async { request: Request[AnyContent] ⇒
        MDC.put("request", f"${request.id}%08x")
        fieldsParser(Field(request)) match {
          case Good(values) ⇒
            req(request).map { r ⇒
              sourceToResult(block(r.map(_ ⇒ Record(values)).asInstanceOf[R[Record[V]]]))
            }
          case Bad(errors) ⇒
            Future.successful(BadRequest(Json.toJson(AttributeCheckingError(errors.toSeq))))
        }
      }
    }

    def iterator[T: Writes](block: R[Record[V]] ⇒ Iterator[T]): Action[AnyContent] =
      actionBuilder.async { request: Request[AnyContent] ⇒
        MDC.put("request", f"${request.id}%08x")
        fieldsParser(Field(request)) match {
          case Good(values) ⇒
            req(request).map { r ⇒
              Results.Ok
                .chunked {
                  Source
                    .fromIterator(() ⇒ block(r.map(_ ⇒ Record(values)).asInstanceOf[R[Record[V]]]))
                    .map(t ⇒ Json.toJson(t).toString)
                    .intersperse("[", ",", "]")
                }
                .as("application/json")
            }
          case Bad(errors) ⇒
            Future.successful(BadRequest(Json.toJson(AttributeCheckingError(errors.toSeq))))
        }
      }

    def iteratorWithTotal[T: Writes](block: R[Record[V]] ⇒ (Int, Iterator[T])): Action[AnyContent] =
      actionBuilder.async { request: Request[AnyContent] ⇒
        MDC.put("request", f"${request.id}%08x")
        fieldsParser(Field(request)) match {
          case Good(values) ⇒
            req(request)
              .map(r ⇒ block(r.map(_ ⇒ Record(values)).asInstanceOf[R[Record[V]]]))
              .map {
                case (total, it) ⇒
                  val res = Results.Ok
                    .chunked {
                      Source
                        .fromIterator(() ⇒ it)
                        .map(t ⇒ Json.toJson(t).toString)
                        .intersperse("[", ",", "]")
                    }
                    .as("application/json")
                  if (total >= 0) res.withHeaders("X-Total" → total.toString)
                  else res
              }
          case Bad(errors) ⇒
            Future.successful(BadRequest(Json.toJson(AttributeCheckingError(errors.toSeq))))
        }
      }

    /**
      * Materialize action using a function that transform request into future response
      *
      * @param block business login function that transform request into future response
      * @return Action
      */
    def async(block: R[Record[V]] ⇒ Future[Result]): Action[AnyContent] =
      actionBuilder.async { request: Request[AnyContent] ⇒
        MDC.put("request", f"${request.id}%08x") // FIXME Future uses other thread
        fieldsParser(Field(request)) match {
          case Good(values) ⇒
            req(request).flatMap { r ⇒
              block(r.map(_ ⇒ Record(values)).asInstanceOf[R[Record[V]]])
            }
          case Bad(errors) ⇒
            Future.successful(BadRequest(Json.toJson(AttributeCheckingError(errors.toSeq))))
        }
      }

    /**
      * Materialize action using a function that transform request into response
      *
      * @param block business login function that transform request into response
      * @return Action
      */
    def apply(block: R[Record[V]] ⇒ Try[Result]): Action[AnyContent] =
      async { r ⇒
        block(r).fold[Future[Result]](errorHandler.onServerError(r.asInstanceOf[RequestHeader], _), Future.successful)
      }
  }

}
