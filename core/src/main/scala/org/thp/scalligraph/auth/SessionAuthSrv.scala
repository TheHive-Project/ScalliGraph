package org.thp.scalligraph.auth

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.Try

import play.api.libs.json.Json
import play.api.mvc.{Request, RequestHeader, Result, Session}
import play.api.{Configuration, Logger}

import javax.inject.{Inject, Singleton}

object ExpirationStatus {
  sealed abstract class Type
  case class Ok(duration: FiniteDuration)      extends Type
  case class Warning(duration: FiniteDuration) extends Type
  case object Error                            extends Type
}

class SessionAuthSrv(
    maxSessionInactivity: FiniteDuration,
    sessionWarning: FiniteDuration,
    userSrv: UserSrv,
    requestOrganisation: RequestOrganisation,
    val ec: ExecutionContext
) extends AuthSrvWithActionFunction {
  override val name: String = "session"
  lazy val logger           = Logger(getClass)

  private def now: Long = System.currentTimeMillis()

  def expirationStatus(request: RequestHeader): ExpirationStatus.Type =
    request
      .session
      .get("expire")
      .flatMap { expireStr =>
        Try(expireStr.toLong).toOption
      }
      .map { expire =>
        (expire - now).millis
      }
      .map {
        case duration if duration.length < 0 => ExpirationStatus.Error
        case duration if duration < sessionWarning =>
          ExpirationStatus.Warning(duration)
        case duration => ExpirationStatus.Ok(duration)
      }
      .getOrElse(ExpirationStatus.Error)

  /**
    * Insert or update session cookie containing user name and session expiration timestamp
    * Cookie is signed by Play framework (it cannot be modified by user)
    */
  override def setSessionUser(authContext: AuthContext): Result => Result = { result: Result =>
    if (result.header.status / 100 == 2) {
      val session = result.newSession.getOrElse(Session()) +
        ("authContext" -> Json.toJson(authContext).toString) +
        ("expire"      -> (now + maxSessionInactivity.toMillis).toString)
      result.withSession(session)
    } else result
  }

  override def getAuthContext[A](request: Request[A]): Option[AuthContext] =
    for {
      authSession <- request
        .session
        .get("authContext")
      if expirationStatus(request) != ExpirationStatus.Error
      authContext <- AuthContext.fromJson(request, authSession).toOption
      orgAuthContext <- requestOrganisation(request) match {
        case Some(organisation) if organisation != authContext.organisation =>
          userSrv.getAuthContext(request, authContext.userId, Some(organisation)).toOption
        case _ => Some(authContext)
      }
    } yield orgAuthContext

  override def transformResult[A](request: Request[A], authContext: AuthContext): Result => Result = setSessionUser(authContext)
}

@Singleton
class SessionAuthProvider @Inject()(userSrv: UserSrv, requestOrganisation: RequestOrganisation, ec: ExecutionContext) extends AuthSrvProvider {
  override val name: String = "session"
  override def apply(config: Configuration): Try[AuthSrv] =
    for {
      maxSessionInactivity <- config.getOrFail[FiniteDuration]("inactivity") // TODO rename settings
      sessionWarning       <- config.getOrFail[FiniteDuration]("warning")
    } yield new SessionAuthSrv(maxSessionInactivity, sessionWarning, userSrv, requestOrganisation, ec)
}
