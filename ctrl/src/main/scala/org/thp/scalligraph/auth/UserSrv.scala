package org.thp.scalligraph.auth

import java.util.concurrent.atomic.AtomicBoolean

import org.thp.scalligraph.{AuthenticationError, AuthorizationError}
import play.api.mvc.RequestHeader

import scala.concurrent.Future

abstract class Permission(val name: String) {
  override def toString: String = name
}

trait AuthContext {
  def userId: String
  def userName: String
  def requestId: String
  def permissions: Seq[Permission]
  private val baseAudit       = new AtomicBoolean(true)
  def getBaseAudit(): Boolean = baseAudit.get && baseAudit.getAndSet(false)
}

trait UserSrv {
  def getFromId(request: RequestHeader, userId: String): Future[AuthContext]
  def getFromUser(request: RequestHeader, user: User): Future[AuthContext]
  def getInitialUser(request: RequestHeader): Future[AuthContext]
//  def inInitAuthContext[A](block: AuthContext ⇒ Future[A]): Future[A]
  val initialAuthContext: AuthContext
  def getUser(userId: String): Future[User]
}

trait User {
  //  val attributes: JsObject
  val id: String
  def getUserName: String
  def getPermissions: Seq[Permission]
}

object AuthCapability extends Enumeration {
  type Type = Value
  val changePassword, setPassword = Value
}
trait AuthSrv {
  val name: String
  val capabilities = Set.empty[AuthCapability.Type]

  def authenticate(username: String, password: String)(implicit request: RequestHeader): Future[AuthContext] =
    Future.failed(AuthenticationError("Operation not supported"))
  def authenticate(key: String)(implicit request: RequestHeader): Future[AuthContext] = Future.failed(AuthenticationError("Operation not supported"))
  def authenticate()(implicit request: RequestHeader): Future[AuthContext]            = Future.failed(AuthenticationError("Operation not supported"))
  def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    Future.failed(AuthorizationError("Operation not supported"))
  def setPassword(username: String, newPassword: String)(implicit authContext: AuthContext): Future[Unit] =
    Future.failed(AuthorizationError("Operation not supported"))
  def renewKey(username: String)(implicit authContext: AuthContext): Future[String] = Future.failed(AuthorizationError("Operation not supported"))
  def getKey(username: String)(implicit authContext: AuthContext): Future[String]   = Future.failed(AuthorizationError("Operation not supported"))
  def removeKey(username: String)(implicit authContext: AuthContext): Future[Unit]  = Future.failed(AuthorizationError("Operation not supported"))
}
