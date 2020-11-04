package org.thp.scalligraph.auth

import javax.inject.{Inject, Singleton}
import javax.naming.directory._
import org.thp.scalligraph.{AuthenticationError, AuthorizationError, EntityIdOrName}
import play.api.mvc.RequestHeader
import play.api.{Configuration, Logger}

import scala.util.{Failure, Success, Try}

case class ADConfig(hosts: Seq[String], useSSL: Boolean, dnsDomain: String, winDomain: String) extends AbstractLdapConfig

class ADAuthSrv(adConfig: ADConfig, userSrv: UserSrv) extends AbstractLdapSrv(adConfig: ADConfig) {
  lazy val logger: Logger                              = Logger(getClass)
  override val name: String                                     = "ad"

  override def authenticate(username: String, password: String, organisation: Option[EntityIdOrName], code: Option[String])(implicit
      request: RequestHeader
  ): Try[AuthContext] =
    connect(adConfig.winDomain + "\\" + username, password)(_ => Success(()))
      .flatMap(_ => userSrv.getAuthContext(request, username, organisation))
      .recoverWith {
        case t => Failure(AuthenticationError("Authentication failure", t))
      }

  override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] = {
    val unicodeOldPassword = ("\"" + oldPassword + "\"").getBytes("UTF-16LE")
    val unicodeNewPassword = ("\"" + newPassword + "\"").getBytes("UTF-16LE")
    connect(adConfig.winDomain + "\\" + username, oldPassword) { ctx =>
      getUserDN(ctx, username).map { userDN =>
        val mods = Array(
          new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("unicodePwd", unicodeOldPassword)),
          new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("unicodePwd", unicodeNewPassword))
        )
        ctx.modifyAttributes(userDN, mods)
      }
    }.recoverWith {
      case t => Failure(AuthorizationError("Change password failure", t))
    }
  }

  override val noLdapServerAvailableException: AuthenticationError = AuthenticationError("No AD server found")

  def getUserDN(ctx: InitialDirContext, username: String): Try[String] =
    Try {
      val controls = new SearchControls()
      controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
      controls.setCountLimit(1)
      val domainDN     = adConfig.dnsDomain.split("\\.").mkString("dc=", ",dc=", "")
      val searchResult = ctx.search(domainDN, "(sAMAccountName={0})", Array[Object](username), controls)
      if (searchResult.hasMore) searchResult.next().getNameInNamespace
      else throw AuthenticationError("User not found in Active Directory")
    }
}

@Singleton
class ADAuthProvider @Inject() (userSrv: UserSrv) extends AuthSrvProvider {
  override val name: String = "ad"
  override def apply(config: Configuration): Try[AuthSrv] =
    for {
      dnsDomain <- config.getOrFail[String]("dnsDomain")
      winDomain <- config.getOrFail[String]("winDomain")
      useSSL    <- config.getOrFail[Boolean]("useSSL")
      hosts    = config.getOptional[Seq[String]]("hosts").getOrElse(Seq(dnsDomain))
      adConfig = ADConfig(hosts, useSSL, dnsDomain, winDomain)
    } yield new ADAuthSrv(adConfig, userSrv)
}
