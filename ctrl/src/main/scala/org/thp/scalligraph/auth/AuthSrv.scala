package org.thp.scalligraph.auth

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

import play.api.Logger
import play.api.mvc.RequestHeader

import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.auth.AuthCapability.Type
import org.thp.scalligraph.{AuthenticationError, AuthorizationError, OAuth2Redirect}

object AuthCapability extends Enumeration {
  type Type = Value
  val changePassword, setPassword = Value
}

trait AuthSrv {
  val name: String
  val capabilities = Set.empty[AuthCapability.Type]

  def authenticate(username: String, password: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    Future.failed(AuthenticationError("Operation not supported"))
  def authenticate(key: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    Future.failed(AuthenticationError("Operation not supported"))
  def authenticate()(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    Future.failed(AuthenticationError("Operation not supported"))
  def changePassword(username: String, oldPassword: String, newPassword: String)(
      implicit authContext: AuthContext,
      ec: ExecutionContext): Future[Unit] = Future.failed(AuthorizationError("Operation not supported"))
  def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext, ec: ExecutionContext): Future[Unit] =
    Future.failed(AuthorizationError("Operation not supported"))
  def renewKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[String] =
    Future.failed(AuthorizationError("Operation not supported"))
  def getKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[String] =
    Future.failed(AuthorizationError("Operation not supported"))
  def removeKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[Unit] =
    Future.failed(AuthorizationError("Operation not supported"))
}

object MultiAuthSrv {
  private[MultiAuthSrv] lazy val logger = Logger(getClass)
}

@Singleton
class MultiAuthSrv @Inject()(val authProviders: immutable.Set[AuthSrv] /*, implicit val ec: ExecutionContext*/ ) extends AuthSrv {
  val name                             = "multi"
  override val capabilities: Set[Type] = authProviders.flatMap(_.capabilities)

  private[auth] def forAllAuthProvider[A](body: AuthSrv ⇒ Future[A])(implicit ec: ExecutionContext) =
    authProviders.foldLeft(Future.failed[A](new Exception("no authentication provider found"))) { (f, a) ⇒
      f.recoverWith {
        case _ ⇒
          val r = body(a)
          r.failed.foreach(error ⇒ MultiAuthSrv.logger.debug(s"${a.name} ${error.getClass.getSimpleName} ${error.getMessage}"))
          r
      }
    }

  override def authenticate(username: String, password: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    forAllAuthProvider(_.authenticate(username, password))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def authenticate(key: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    forAllAuthProvider(_.authenticate(key))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def authenticate()(implicit request: RequestHeader, ec: ExecutionContext): Future[AuthContext] =
    forAllAuthProvider(_.authenticate)
      .recoverWith {
        case e: OAuth2Redirect ⇒ Future.failed(e)
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Future.failed(AuthenticationError("Authentication failure"))
      }

  override def changePassword(username: String, oldPassword: String, newPassword: String)(
      implicit authContext: AuthContext,
      ec: ExecutionContext): Future[Unit] =
    forAllAuthProvider(_.changePassword(username, oldPassword, newPassword))

  override def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext, ec: ExecutionContext): Future[Unit] =
    forAllAuthProvider(_.setPassword(username, newPassword))

  override def renewKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[String] =
    forAllAuthProvider(_.renewKey(username))

  override def getKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[String] =
    forAllAuthProvider(_.getKey(username))

  override def removeKey(username: String)(implicit request: RequestHeader, ec: ExecutionContext): Future[Unit] =
    forAllAuthProvider(_.removeKey(username))
}
