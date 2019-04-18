package org.thp.scalligraph.controllers

import scala.util.{Failure, Success, Try}

import play.api.mvc.{RequestHeader, Result}

import javax.inject.Inject
import org.thp.scalligraph.AuthenticationError
import org.thp.scalligraph.auth.{AuthContext, UserSrv}

class TestAuthenticateSrv @Inject()(userSrv: UserSrv) extends AuthenticateSrv {
  override def getAuthContext(request: RequestHeader): Try[AuthContext] =
    for {
      user        ← request.headers.get("user").fold[Try[String]](Failure(AuthenticationError("User header not found")))(Success.apply)
      authContext ← userSrv.getFromId(request, user)
    } yield authContext

  override def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result = result
}
