package org.thp.scalligraph.controllers

import org.thp.scalligraph.auth.{HeaderAuthSrv, RequestOrganisation, UserSrv}
import play.api.Configuration

import scala.concurrent.ExecutionContext

class TestAuthSrv(userSrv: UserSrv, ec: ExecutionContext)
    extends HeaderAuthSrv("user", new RequestOrganisation(Configuration.apply("auth.organisationHeader" -> "X-Organisation")), None, userSrv, ec)
