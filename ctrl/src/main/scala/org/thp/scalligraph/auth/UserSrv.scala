package org.thp.scalligraph.auth

import scala.concurrent.Future

import play.api.libs.json.{JsString, Writes}
import play.api.mvc.RequestHeader

abstract class Permission(val name: String) {
  override def toString: String = name
}
object Permission {
  implicit val writes: Writes[Permission] = Writes[Permission](p ⇒ JsString(p.name))
}

trait AuthContext {
  def userId: String
  def userName: String
  def organisation: String
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
