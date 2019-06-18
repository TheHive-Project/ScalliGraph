package org.thp.scalligraph.controllers

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import play.api.Logger
import play.api.http.HttpErrorHandler
import play.api.libs.json.Json
import play.api.mvc.Results.BadRequest
import play.api.mvc._

import akka.stream.Materializer
import gremlin.scala.Graph
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.AttributeCheckingError
import org.thp.scalligraph.models.Database
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
    implicit val mat: Materializer
) {

  lazy val logger = Logger(getClass)

  /**
    * Create a named entry point
    *
    * @param name name of entry point
    * @return empty entry point
    */
  def apply(name: String): EntryPointBuilder[HNil] =
    EntryPointBuilder[HNil](name, FieldsParser.good(HNil))

  /**
    * An entry point is defined by its name, a fields parser which transform request into a record V and the type of request (
    * authenticated or not)
    *
    * @param name         name of the entry point
    * @param fieldsParser fields parser that transform request into a record of type V
    * @tparam V type of record
    */
  case class EntryPointBuilder[V <: HList](
      name: String,
      fieldsParser: FieldsParser[V]
  ) {

    /**
      * Extract a field from request.
      *
      * @param fp field parser to use to extract value from request
      * @tparam T type of extracted field
      * @return a new entry point with added fields parser
      */
    def extract[N, T](fieldName: Witness.Aux[N], fp: FieldsParser[T]): EntryPointBuilder[FieldType[N, T] :: V] =
      EntryPointBuilder(name, fieldsParser.andThen(fieldName.toString)(fp)(labelled.field[N](_) :: _))

    /**
      * Add an authentication check to this entry point.
      *
      * @return a new entry point with added authentication check
      */
    def auth(block: AuthenticatedRequest[Record[V]] ⇒ Try[Result]): Action[AnyContent] =
      actionBuilder.async { request ⇒
        val result = for {
          authContext ← authenticateSrv.getAuthContext(request)
          values      ← fieldsParser(Field(request)).badMap(errors ⇒ AttributeCheckingError(errors.toSeq)).toTry
          authRequest = new AuthenticatedRequest(authContext, request.map(_ ⇒ Record(values)))
          result ← block(authRequest)
          authResult = authenticateSrv.setSessingUser(result, authContext)(request)
        } yield authResult
        result.fold[Future[Result]](errorHandler.onServerError(request, _), Future.successful)
      }

    /**
      * Add async auth check to this entry point
      *
      * @param block the action body block returning a future
      * @return
      */
    def asyncAuth(block: AuthenticatedRequest[Record[V]] ⇒ Future[Result]): Action[AnyContent] =
      actionBuilder.async { request ⇒
        val result = for {
          authContext ← Future.fromTry(authenticateSrv.getAuthContext(request))
          values      ← Future.fromTry(fieldsParser(Field(request)).badMap(errors ⇒ AttributeCheckingError(errors.toSeq)).toTry)
          authRequest = new AuthenticatedRequest(authContext, request.map(_ ⇒ Record(values)))
          result ← block(authRequest)
          authResult = authenticateSrv.setSessingUser(result, authContext)(request)
        } yield authResult

        result
      }

    def authTransaction(
        db: Database
    )(block: AuthenticatedRequest[Record[V]] ⇒ Graph ⇒ Try[Result]): Action[AnyContent] =
      apply { request ⇒
        val result = authenticateSrv.getAuthContext(request).flatMap { authContext ⇒
          val authReq = new AuthenticatedRequest(authContext, request)
          db.tryTransaction(graph ⇒ block(authReq)(graph))
            .map(result ⇒ authenticateSrv.setSessingUser(result, authContext)(request))
        }
        result
      }

    /**
      * Materialize action using a function that transform request into response
      *
      * @param block business login function that transform request into response
      * @return Action
      */
    def apply(block: Request[Record[V]] ⇒ Try[Result]): Action[AnyContent] =
      async { r ⇒
        block(r)
          .fold[Future[Result]](errorHandler.onServerError(r, _), Future.successful)
      }

    /**
      * Materialize action using a function that transform request into future response
      *
      * @param block business login function that transform request into future response
      * @return Action
      */
    def async(block: Request[Record[V]] ⇒ Future[Result]): Action[AnyContent] =
      actionBuilder.async { request ⇒
        fieldsParser(Field(request))
          .fold[Future[Result]](
            values ⇒ block(request.map(_ ⇒ Record(values))),
            errors ⇒ Future.successful(BadRequest(Json.toJson(AttributeCheckingError(errors.toSeq))))
          )
      }
  }
}
