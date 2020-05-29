package org.thp.scalligraph.controllers

import scala.concurrent.ExecutionContext

import javax.inject.Inject
import org.thp.scalligraph.auth.{HeaderAuthSrv, RequestOrganisation, UserSrv}

class TestAuthSrv @Inject() (userSrv: UserSrv, ec: ExecutionContext)
    extends HeaderAuthSrv("user", new RequestOrganisation(Some("X-Organisation"), None, None, None), None, userSrv, ec)
