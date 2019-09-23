package org.thp.scalligraph.auth

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import play.api.Configuration
import play.api.mvc.{ActionFunction, Request, Result}

import javax.inject.Inject
import org.thp.scalligraph.controllers.AuthenticatedRequest

class HeaderAuthSrv(userHeader: String, requestOrganisation: RequestOrganisation, userSrv: UserSrv, ec: ExecutionContext) extends AuthSrv {
  override val name: String = "header"
  override def actionFunction(nextFunction: ActionFunction[Request, AuthenticatedRequest]): ActionFunction[Request, AuthenticatedRequest] =
    new ActionFunction[Request, AuthenticatedRequest] {
      override def invokeBlock[A](request: Request[A], block: AuthenticatedRequest[A] => Future[Result]): Future[Result] =
        request
          .headers
          .get(userHeader)
          .flatMap(userSrv.getAuthContext(request, _, requestOrganisation(request)).toOption)
          .fold(nextFunction.invokeBlock(request, block)) { authContext =>
            block(new AuthenticatedRequest[A](authContext, request))
          }
      override protected def executionContext: ExecutionContext = ec
    }
}

class HeaderAuthProvider @Inject()(requestOrganisation: RequestOrganisation, userSrv: UserSrv, ec: ExecutionContext) extends AuthSrvProvider {
  override val name: String = "header"
  override def apply(config: Configuration): Try[AuthSrv] =
    for {
      userHeader <- config.getOrFail[String]("userHeader")
    } yield new HeaderAuthSrv(userHeader, requestOrganisation, userSrv, ec)
}
