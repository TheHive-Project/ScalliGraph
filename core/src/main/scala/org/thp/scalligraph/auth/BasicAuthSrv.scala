package org.thp.scalligraph.auth
import java.util.Base64

import javax.inject.{Inject, Provider, Singleton}
import play.api.Configuration
import play.api.http.HeaderNames
import play.api.mvc.Request

import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

class BasicAuthSrv(authSrv: AuthSrv, requestOrganisation: RequestOrganisation, val ec: ExecutionContext) extends AuthSrvWithActionFunction {

  override val name: String = "basic"

  override def getAuthContext[A](request: Request[A]): Option[AuthContext] =
    request
      .headers
      .get(HeaderNames.AUTHORIZATION)
      .collect {
        case h if h.startsWith("Basic ") =>
          val authWithoutBasic = h.substring(6)
          val decodedAuth      = new String(Base64.getDecoder.decode(authWithoutBasic), "UTF-8")
          decodedAuth.split(":")
      }
      .flatMap {
        case Array(username, password) => authSrv.authenticate(username, password, requestOrganisation(request))(request).toOption
        case _                         => None
      }
}

@Singleton
class BasicAuthProvider @Inject()(authSrvProvider: Provider[AuthSrv], requestOrganisation: RequestOrganisation, ec: ExecutionContext)
    extends AuthSrvProvider {
  lazy val authSrv: AuthSrv = authSrvProvider.get
  override val name: String = "basic"
  override def apply(config: Configuration): Try[AuthSrv] =
    Success(new BasicAuthSrv(authSrv, requestOrganisation, ec))
}
