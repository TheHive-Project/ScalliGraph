package org.thp.scalligraph.auth

import org.thp.scalligraph.utils.Instance
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsObject, JsPath, Json, Reads, Writes}
import play.api.mvc.RequestHeader

import scala.util.Try

trait PermissionTag

object Permission {
  def apply(name: String): Permission            = shapeless.tag[PermissionTag][String](name)
  def apply(names: Set[String]): Set[Permission] = names.map(apply)
}

trait AuthContext {
  def userId: String
  def userName: String
  def organisation: String
  def requestId: String
  def permissions: Set[Permission]
  def changeOrganisation(newOrganisation: String): AuthContext
}

case class AuthContextImpl(userId: String, userName: String, organisation: String, requestId: String, permissions: Set[Permission])
    extends AuthContext {
  override def changeOrganisation(newOrganisation: String): AuthContext = copy(organisation = newOrganisation)
}

object AuthContext {

  def fromJson(request: RequestHeader, json: String): Try[AuthContext] =
    Try {
      Json.parse(json).as(reads(Instance.getRequestId(request)))
    }

  def reads(requestId: String): Reads[AuthContext] =
    ((JsPath \ "userId").read[String] and
      (JsPath \ "userName").read[String] and
      (JsPath \ "organisation").read[String] and
      Reads.pure(requestId) and
      (JsPath \ "permissions").read[Set[String]].map(Permission.apply))(AuthContextImpl.apply _)

  implicit val writes: Writes[AuthContext] = Writes[AuthContext] { authContext =>
    Json.obj(
      "userId"       -> authContext.userId,
      "userName"     -> authContext.userName,
      "organisation" -> authContext.organisation,
      "permissions"  -> authContext.permissions
    )
  }
}

trait UserSrv {
  def getAuthContext(request: RequestHeader, userId: String, organisationName: Option[String]): Try[AuthContext]
  def getSystemAuthContext: AuthContext
  def createUser(userId: String, userInfo: JsObject): Try[User]
}

trait User {
  val id: String
  def getUserName: String
}
