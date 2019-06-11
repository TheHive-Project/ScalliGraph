package org.thp.scalligraph.auth

import scala.collection.immutable
import scala.util.{Failure, Try}

import play.api.Logger
import play.api.mvc.RequestHeader

import javax.inject.{Inject, Provider, Singleton}
import org.thp.scalligraph.{AuthenticationError, AuthorizationError, OAuth2Redirect}

object AuthCapability extends Enumeration {
  val changePassword, setPassword, authByKey = Value
}

trait AuthSrv {
  val name: String
  val capabilities = Set.empty[AuthCapability.Value]

  def authenticate(username: String, password: String, organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    Failure(AuthenticationError("Operation not supported"))

  def authenticate(key: String, organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    Failure(AuthenticationError("Operation not supported"))

  def authenticate(organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    Failure(AuthenticationError("Operation not supported"))

  def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] =
    Failure(AuthorizationError("Operation not supported"))

  def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] =
    Failure(AuthorizationError("Operation not supported"))

  def renewKey(username: String)(implicit authContext: AuthContext): Try[String] =
    Failure(AuthorizationError("Operation not supported"))

  def getKey(username: String)(implicit authContext: AuthContext): Try[String] =
    Failure(AuthorizationError("Operation not supported"))

  def removeKey(username: String)(implicit authContext: AuthContext): Try[Unit] =
    Failure(AuthorizationError("Operation not supported"))
}

object MultiAuthSrv {
  private[MultiAuthSrv] lazy val logger = Logger(getClass)
}

@Singleton
class MultiAuthSrvProvider @Inject()(val authProviders: immutable.Set[AuthSrv]) extends Provider[AuthSrv] {
  override def get(): AuthSrv = new MultiAuthSrv(authProviders)
}

class MultiAuthSrv(val authProviders: immutable.Set[AuthSrv]) extends AuthSrv {
  val name: String                                     = "multi"
  override val capabilities: Set[AuthCapability.Value] = authProviders.flatMap(_.capabilities)

  private[auth] def forAllAuthProvider[A](body: AuthSrv ⇒ Try[A]): Try[A] =
    authProviders.foldLeft[Try[A]](Failure(new Exception("no authentication provider found"))) { (f, a) ⇒
      f.recoverWith {
        case _ ⇒
          val r = body(a)
          r.failed.foreach(error ⇒ MultiAuthSrv.logger.debug(s"${a.name} ${error.getClass.getSimpleName} ${error.getMessage}"))
          r
      }
    }

  override def authenticate(username: String, password: String, organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    forAllAuthProvider(_.authenticate(username, password, organisation))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Failure(AuthenticationError("Authentication failure"))
      }

  override def authenticate(key: String, organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    forAllAuthProvider(_.authenticate(key, organisation))
      .recoverWith {
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Failure(AuthenticationError("Authentication failure"))
      }

  override def authenticate(organisation: Option[String])(implicit request: RequestHeader): Try[AuthContext] =
    forAllAuthProvider(_.authenticate(organisation: Option[String]))
      .recoverWith {
        case e: OAuth2Redirect ⇒ Failure(e)
        case authError ⇒
          MultiAuthSrv.logger.error("Authentication failure", authError)
          Failure(AuthenticationError("Authentication failure"))
      }

  override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] =
    forAllAuthProvider(_.changePassword(username, oldPassword, newPassword))

  override def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] =
    forAllAuthProvider(_.setPassword(username, newPassword))

  override def renewKey(username: String)(implicit authContext: AuthContext): Try[String] =
    forAllAuthProvider(_.renewKey(username))

  override def getKey(username: String)(implicit authContext: AuthContext): Try[String] =
    forAllAuthProvider(_.getKey(username))

  override def removeKey(username: String)(implicit authContext: AuthContext): Try[Unit] =
    forAllAuthProvider(_.removeKey(username))
}
