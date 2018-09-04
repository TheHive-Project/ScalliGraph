package org.thp.scalligraph.services.auth

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

import play.api.Logger
import play.api.mvc.RequestHeader

import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.auth.AuthCapability.Type
import org.thp.scalligraph.auth.{AuthContext, AuthSrv}
import org.thp.scalligraph.{AuthenticationError, OAuth2Redirect}

object MultiAuthSrv {
  private[MultiAuthSrv] lazy val logger = Logger(getClass)
}

@Singleton
class MultiAuthSrv @Inject()(val authProviders: immutable.Set[AuthSrv], implicit val ec: ExecutionContext) extends AuthSrv {

  val name                             = "multi"
  override val capabilities: Set[Type] = authProviders.flatMap(_.capabilities)

  private[auth] def forAllAuthProvider[A](body: AuthSrv ⇒ Future[A]) =
    authProviders.foldLeft(Future.failed[A](new Exception("no authentication provider found"))) { (f, a) ⇒
      f.recoverWith {
        case _ ⇒
          val r = body(a)
          r.failed.foreach(error ⇒ MultiAuthSrv.logger.debug(s"${a.name} ${error.getClass.getSimpleName} ${error.getMessage}"))
          r
      }
    }

  override def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] =
    forAllAuthProvider(_.authenticate(username, password))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def authenticate(key: String)(implicit request: RequestHeader): Future[AuthContext] =
    forAllAuthProvider(_.authenticate(key))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def authenticate()(implicit request: RequestHeader): Future[AuthContext] =
    forAllAuthProvider(_.authenticate)
      .recoverWith {
        case e: OAuth2Redirect ⇒ Future.failed(e)
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    forAllAuthProvider(_.changePassword(username, oldPassword, newPassword))

  override def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    forAllAuthProvider(_.setPassword(username, newPassword))

  override def renewKey(username: String)(implicit authContext: AuthContext): Future[String] =
    forAllAuthProvider(_.renewKey(username))

  override def getKey(username: String)(implicit authContext: AuthContext): Future[String] =
    forAllAuthProvider(_.getKey(username))

  override def removeKey(username: String)(implicit authContext: AuthContext): Future[Unit] =
    forAllAuthProvider(_.removeKey(username))
}
