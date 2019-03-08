package org.thp.scalligraph.models

import org.thp.scalligraph.auth.{AuthContext, Permission, User, UserSrv}
import play.api.mvc.RequestHeader
import scala.util.{Success, Try}

case class DummyUserSrv(
    userId: String = "test",
    userName: String = "test user",
    organisation: String = "test_organisation",
    permissions: Seq[Permission] = Nil,
    requestId: String = "testRequest")
    extends UserSrv { userSrv ⇒

  val authContext: AuthContext = new AuthContext {
    override def userId: String               = userSrv.userId
    override def userName: String             = userSrv.userName
    override def organisation: String         = userSrv.organisation
    override def permissions: Seq[Permission] = userSrv.permissions
    override def requestId: String            = userSrv.requestId
  }
  override def getFromId(request: RequestHeader, userId: String): Try[AuthContext] = Success(authContext)

  override def getFromUser(request: RequestHeader, user: User): Try[AuthContext] = Success(authContext)

  override def getInitialUser(request: RequestHeader): Try[AuthContext] = Success(authContext)

  override val initialAuthContext: AuthContext = authContext

  override def getUser(userId: String): Try[User] = ???
}
