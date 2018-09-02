package org.thp.scalligraph.models

import org.thp.scalligraph.auth.{AuthContext, Permission, User, UserSrv}
import play.api.mvc.RequestHeader

import scala.concurrent.Future

case class DummyUserSrv(
    userId: String = "test",
    userName: String = "test user",
    permissions: Seq[Permission] = Nil,
    requestId: String = "testRequest")
    extends UserSrv { userSrv ⇒

  val authContext: AuthContext = new AuthContext {
    override def userId: String               = userSrv.userId
    override def userName: String             = userSrv.userName
    override def permissions: Seq[Permission] = userSrv.permissions
    override def requestId: String            = userSrv.requestId
  }
  override def getFromId(request: RequestHeader, userId: String): Future[AuthContext] = Future.successful(authContext)

  override def getFromUser(request: RequestHeader, user: User): Future[AuthContext] = Future.successful(authContext)

  override def getInitialUser(request: RequestHeader): Future[AuthContext] = Future.successful(authContext)

  override val initialAuthContext: AuthContext = authContext

  override def getUser(userId: String): Future[User] = ???
}
