package org.thp.scalligraph.auth

import javax.inject.{Inject, Singleton}
import javax.naming.directory._
import org.thp.scalligraph.{AuthenticationError, AuthorizationError, EntityIdOrName}
import play.api.mvc.RequestHeader
import play.api.{Configuration, Logger}

import scala.util.{Failure, Success, Try}

case class BasicLdapConfig(hosts: Seq[String], useSSL: Boolean, bindDN: String, bindPW: String, baseDN: String, filter: String) extends AbstractLdapConfig

class LdapAuthSrv(ldapConfig: BasicLdapConfig, userSrv: UserSrv) extends AbstractLdapSrv(ldapConfig: BasicLdapConfig) {
  lazy val logger: Logger                              = Logger(getClass)
  val name                                             = "ldap"

  override def authenticate(username: String, password: String, organisation: Option[EntityIdOrName], code: Option[String])(implicit
      request: RequestHeader
  ): Try[AuthContext] =
    connect(ldapConfig.bindDN, ldapConfig.bindPW)(ctx => getUserDN(ctx, username))
      .flatMap(userDN => connect(userDN, password)(_ => Success(())))
      .flatMap(_ => userSrv.getAuthContext(request, username, organisation))
      .recoverWith { case AuthenticationError(_, cause) if cause != null => Failure(cause) }

  override def changePassword(username: String, oldPassword: String, newPassword: String)(implicit authContext: AuthContext): Try[Unit] =
    connect(ldapConfig.bindDN, ldapConfig.bindPW)(ctx => getUserDN(ctx, username))
      .flatMap { userDN =>
        connect(userDN, oldPassword) { ctx =>
          val mods = Array(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("userPassword", newPassword)))
          Try(ctx.modifyAttributes(userDN, mods))
        }
      }
      .recoverWith {
        case t => Failure(AuthorizationError("Change password failure", t))
      }

  def getUserDN(ctx: InitialDirContext, username: String): Try[String] =
    Try {
      val controls = new SearchControls()
      controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
      controls.setCountLimit(1)
      val searchResult = ctx.search(ldapConfig.baseDN, ldapConfig.filter, Array[Object](username), controls)
      if (searchResult.hasMore) searchResult.next().getNameInNamespace
      else throw AuthenticationError("User not found in LDAP server")
    }
}

@Singleton
class LdapAuthProvider @Inject() (userSrv: UserSrv) extends AuthSrvProvider {
  override val name: String = "ldap"
  override def apply(config: Configuration): Try[AuthSrv] =
    for {
      bindDN <- config.getOrFail[String]("bindDN")
      bindPW <- config.getOrFail[String]("bindPW")
      baseDN <- config.getOrFail[String]("baseDN")
      filter <- config.getOrFail[String]("filter")
      hosts  <- config.getOrFail[Seq[String]]("hosts")
      useSSL <- config.getOrFail[Boolean]("useSSL")
      ldapConfig = BasicLdapConfig(hosts, useSSL, bindDN, bindPW, baseDN, filter)
    } yield new LdapAuthSrv(ldapConfig, userSrv)
}
