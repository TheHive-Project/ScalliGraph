package org.thp.scalligraph.auth

import java.net.ConnectException
import java.util

import javax.naming.Context
import javax.naming.directory.InitialDirContext
import org.thp.scalligraph.AuthenticationError
import play.api.Logger

import scala.util.{Failure, Try}

abstract class AbstractLdapConfig {
  def hosts: Seq[String]
  def useSSL: Boolean
}

abstract class AbstractLdapSrv[A <: AbstractLdapConfig](config: A) extends AuthSrv {
  def logger: Logger
  override val capabilities: Set[AuthCapability.Value] = Set(AuthCapability.changePassword)

  val noLdapServerAvailableException: AuthenticationError = AuthenticationError("No LDAP server found")

  @scala.annotation.tailrec
  final def isFatal(t: Throwable): Boolean =
    t match {
      case null                             => true
      case `noLdapServerAvailableException` => false
      case _: ConnectException              => false
      case _                                => isFatal(t.getCause)
    }

  def connect[B](username: String, password: String)(f: InitialDirContext => Try[B]): Try[B] =
    config.hosts.foldLeft[Try[B]](Failure(noLdapServerAvailableException)) {
      case (Failure(e), serverName) if !isFatal(e) =>
        val protocol = if (config.useSSL) "ldaps://" else "ldap://"
        val env      = new util.Hashtable[Any, Any]
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
        env.put(Context.PROVIDER_URL, protocol + serverName)
        env.put(Context.SECURITY_AUTHENTICATION, "simple")
        env.put(Context.SECURITY_PRINCIPAL, username)
        env.put(Context.SECURITY_CREDENTIALS, password)
        Try {
          val ctx = new InitialDirContext(env)
          try f(ctx)
          finally ctx.close()
        }.flatten
      case (failure @ Failure(e), _) =>
        logger.debug("LDAP connect error", e)
        failure
      case (r, _) => r
    }

  def getUserDN(ctx: InitialDirContext, username: String): Try[String]

}
