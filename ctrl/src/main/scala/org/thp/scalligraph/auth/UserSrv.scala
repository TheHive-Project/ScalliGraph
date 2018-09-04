package org.thp.scalligraph.auth

import scala.concurrent.Future

import play.api.libs.json.{JsString, Writes}
import play.api.mvc.RequestHeader

import org.thp.scalligraph.{AuthenticationError, AuthorizationError}

abstract class Permission(val name: String) {
  override def toString: String = name
}
object Permission {
  implicit val writes: Writes[Permission] = Writes[Permission](p ⇒ JsString(p.name))
}

trait AuthContext {
  def userId: String
  def userName: String
  def requestId: String
  def permissions: Seq[Permission]
}

trait UserSrv {
  def getFromId(request: RequestHeader, userId: String): Future[AuthContext]
  def getFromUser(request: RequestHeader, user: User): Future[AuthContext]
  def getInitialUser(request: RequestHeader): Future[AuthContext]
  val initialAuthContext: AuthContext
  def getUser(userId: String): Future[User]
}

trait User {
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
