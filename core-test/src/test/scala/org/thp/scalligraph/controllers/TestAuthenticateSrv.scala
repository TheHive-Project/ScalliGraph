package org.thp.scalligraph.controllers

import scala.util.{Failure, Success, Try}

import play.api.Configuration
import play.api.mvc.{RequestHeader, Result}

import javax.inject.Inject
import org.thp.scalligraph.AuthenticationError
import org.thp.scalligraph.auth.{AuthContext, UserSrv}

class TestAuthenticateSrv @Inject()(configuration: Configuration, userSrv: UserSrv) extends AuthenticateSrv {
  val organisationHeader: String = configuration.get[String]("auth.organisationHeader")

  override def getAuthContext(request: RequestHeader): Try[AuthContext] =
    for {
      user ← request.headers.get("user").fold[Try[String]](Failure(AuthenticationError("User header not found")))(Success.apply)
      organisation = request.headers.get(organisationHeader)
      authContext ← userSrv.getFromId(request, user, organisation)
    } yield authContext

  override def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result = result
}
