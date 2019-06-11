package org.thp.scalligraph.controllers

import java.io.ByteArrayInputStream
import java.util.{List ⇒ JList}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.{Failure, Success, Try}

import play.api.http.HeaderNames
import play.api.libs.json.Json
import play.api.mvc._
import play.api.{Configuration, Logger}

import javax.inject.{Inject, Singleton}
import javax.naming.ldap.LdapName
import org.bouncycastle.asn1._
import org.thp.scalligraph.auth._
import org.thp.scalligraph.{AuthenticationError, Instance}

/**
  * A request with authentication information
  *
  * @param authContext authentication information (which contains user name, permissions, ...)
  * @param request the request
  * @tparam A the body content type.
  */
class AuthenticatedRequest[A](val authContext: AuthContext, request: Request[A]) extends WrappedRequest[A](request) with AuthContext with Request[A] {
  override def userId: String               = authContext.userId
  override def userName: String             = authContext.userName
  override def organisation: String         = authContext.organisation
  override def requestId: String            = Instance.getRequestId(request)
  override def permissions: Set[Permission] = authContext.permissions
  override def map[B](f: A ⇒ B): AuthenticatedRequest[B] =
    new AuthenticatedRequest(authContext, request.map(f))
}

object ExpirationStatus {
  sealed abstract class Type
  case class Ok(duration: FiniteDuration)      extends Type
  case class Warning(duration: FiniteDuration) extends Type
  case object Error                            extends Type
}

trait AuthenticateSrv {
  def getAuthContext(request: RequestHeader): Try[AuthContext]
  def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result
}

/**
  * Check and manage user security authentication
  */
@Singleton
class DefaultAuthenticateSrv @Inject()(configuration: Configuration, userSrv: UserSrv, authSrv: AuthSrv, defaultParser: BodyParsers.Default)
    extends AuthenticateSrv {

  val maxSessionInactivity: FiniteDuration = configuration.getMillis("session.inactivity").millis
  val sessionWarning: FiniteDuration       = configuration.getMillis("session.warning").millis
  val certificateField: Option[String]     = configuration.getOptional[String]("auth.pki.certificateField")
  val authBySessionCookie: Boolean         = configuration.get[Boolean]("auth.method.session")
  val authByKey: Boolean                   = configuration.get[Boolean]("auth.method.key")
  val authByBasicAuth: Boolean             = configuration.get[Boolean]("auth.method.basic")
  val authByInitialUser: Boolean           = configuration.get[Boolean]("auth.method.init")
  val authByPki: Boolean                   = configuration.get[Boolean]("auth.method.pki")
  val organisationHeader: String           = configuration.get[String]("auth.organisationHeader")
  val authContextSession: String           = "authContext"

  lazy val logger = Logger(getClass)

  private def now: Long = System.currentTimeMillis()

  private def requestOrganisation(request: RequestHeader): Option[String] = request.headers.get(organisationHeader)

  /**
    * Insert or update session cookie containing user name and session expiration timestamp
    * Cookie is signed by Play framework (it cannot be modified by user)
    */
  override def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result =
    result.addingToSession(authContextSession → Json.toJson(authContext).toString, "expire" → (now + maxSessionInactivity.toMillis).toString)

  /**
    * Retrieve authentication information form cookie
    */
  def getFromSession(request: RequestHeader): Try[AuthContext] =
    request
      .session
      .get(authContextSession)
      .fold[Try[AuthContext]](Failure(AuthenticationError("User session not found"))) {
        case authContextJson if expirationStatus(request) != ExpirationStatus.Error ⇒
          AuthContext
            .fromJson(request, authContextJson)
            .recoverWith {
              case error ⇒
                logger.error("Authentication context can't be decoded from user session", error)
                Failure(AuthenticationError("Invalid user session"))
            }
        case _ ⇒ Failure(AuthenticationError("User session has expired"))
      }

  def expirationStatus(request: RequestHeader): ExpirationStatus.Type =
    request
      .session
      .get("expire")
      .flatMap { expireStr ⇒
        Try(expireStr.toLong).toOption
      }
      .map { expire ⇒
        (expire - now).millis
      }
      .map {
        case duration if duration.length < 0 ⇒ ExpirationStatus.Error
        case duration if duration < sessionWarning ⇒
          ExpirationStatus.Warning(duration)
        case duration ⇒ ExpirationStatus.Ok(duration)
      }
      .getOrElse(ExpirationStatus.Error)

  /**
    * Retrieve authentication information from API key
    */
  def getFromApiKey(request: RequestHeader): Try[AuthContext] =
    for {
      auth ← request
        .headers
        .get(HeaderNames.AUTHORIZATION)
        .fold[Try[String]](Failure(AuthenticationError("Authentication header not found")))(Success.apply)
      _ ← if (!auth.startsWith("Bearer ")) Failure(AuthenticationError("Only bearer authentication is supported")) else Success(())
      key = auth.substring(7)
      authContext ← authSrv.authenticate(key, requestOrganisation(request))(request)
    } yield authContext

  def getFromBasicAuth(request: RequestHeader): Try[AuthContext] =
    for {
      auth ← request
        .headers
        .get(HeaderNames.AUTHORIZATION)
        .fold[Try[String]](Failure(AuthenticationError("Authentication header not found")))(Success.apply)
      _ ← if (!auth.startsWith("Basic ")) Failure(AuthenticationError("Only basic authentication is supported")) else Success(())
      authWithoutBasic = auth.substring(6)
      decodedAuth      = new String(java.util.Base64.getDecoder.decode(authWithoutBasic), "UTF-8")
      authContext ← decodedAuth.split(":") match {
        case Array(username, password) ⇒ authSrv.authenticate(username, password, requestOrganisation(request))(request)
        case _                         ⇒ Failure(AuthenticationError("Can't decode authentication header"))
      }
    } yield authContext

  private def asn1String(obj: ASN1Primitive): String = obj match {
    case ds: DERUTF8String    ⇒ DERUTF8String.getInstance(ds).getString
    case to: ASN1TaggedObject ⇒ asn1String(ASN1TaggedObject.getInstance(to).getObject)
    case os: ASN1OctetString  ⇒ new String(os.getOctets)
    case as: ASN1String       ⇒ as.getString
  }

  private object CertificateSAN {

    def unapply(l: JList[_]): Option[(String, String)] = {
      val typeValue = for {
        t ← Option(l.get(0))
        v ← Option(l.get(1))
      } yield t → v
      typeValue
        .collect { case (t: Integer, v) ⇒ t.toInt → v }
        .collect {
          case (0, value: Array[Byte]) ⇒
            val asn1     = new ASN1InputStream(new ByteArrayInputStream(value)).readObject()
            val asn1Seq  = ASN1Sequence.getInstance(asn1)
            val id       = ASN1ObjectIdentifier.getInstance(asn1Seq.getObjectAt(0)).getId
            val valueStr = asn1String(asn1Seq.getObjectAt(1).toASN1Primitive)

            id match {
              case "1.3.6.1.4.1.311.20.2.3" ⇒ "upn" → valueStr
              // Add other object id
              case other ⇒ other → valueStr
            }
          case (1, value: String) ⇒ "rfc822Name"                → value
          case (2, value: String) ⇒ "dNSName"                   → value
          case (3, value: String) ⇒ "x400Address"               → value
          case (4, value: String) ⇒ "directoryName"             → value
          case (5, value: String) ⇒ "ediPartyName"              → value
          case (6, value: String) ⇒ "uniformResourceIdentifier" → value
          case (7, value: String) ⇒ "iPAddress"                 → value
          case (8, value: String) ⇒ "registeredID"              → value
        }
    }
  }

  def getFromClientCertificate(request: RequestHeader): Try[AuthContext] =
    certificateField
      .fold[Try[AuthContext]](Failure(AuthenticationError("Certificate authentication is not configured"))) { cf ⇒
        request
          .clientCertificateChain
          .flatMap(_.headOption)
          .flatMap { cert ⇒
            val dn       = cert.getSubjectX500Principal.getName
            val ldapName = new LdapName(dn)
            ldapName
              .getRdns
              .asScala
              .collectFirst {
                case rdn if rdn.getType == cf ⇒ userSrv.getFromId(request, rdn.getValue.toString, requestOrganisation(request))
              }
              .orElse {
                for {
                  san ← Option(cert.getSubjectAlternativeNames)
                  fieldValue ← san.asScala.collectFirst {
                    case CertificateSAN(`cf`, value) ⇒ userSrv.getFromId(request, value, requestOrganisation(request))
                  }
                } yield fieldValue
              }
          }
          .getOrElse(Failure(AuthenticationError("Certificate doesn't contain user information")))
      }

  val authenticationMethods: Seq[(String, RequestHeader ⇒ Try[AuthContext])] =
    (if (authBySessionCookie) Seq("session" → getFromSession _) else Nil) ++
      (if (authByPki) Seq("pki"             → getFromClientCertificate _) else Nil) ++
      (if (authByKey) Seq("key"             → getFromApiKey _) else Nil) ++
      (if (authByBasicAuth) Seq("basic"     → getFromBasicAuth _) else Nil)

  override def getAuthContext(request: RequestHeader): Try[AuthContext] =
    authenticationMethods
      .foldLeft[Try[Either[Seq[(String, Throwable)], AuthContext]]](Success(Left(Nil))) {
        case (acc, (authMethodName, authMethod)) ⇒
          acc.flatMap {
            case authContext if authContext.isRight ⇒ Success(authContext)
            case Left(errors) ⇒
              authMethod(request)
                .map(authContext ⇒ Right(authContext))
                .recover { case error ⇒ Left(errors :+ (authMethodName → error)) }
          }
      }
      .flatMap {
        case Right(authContext) ⇒ Success(authContext)
        case Left(errors) ⇒
          val errorDetails = errors
            .map { case (authMethodName, error) ⇒ s"\t$authMethodName: ${error.getClass.getSimpleName} ${error.getMessage}" }
            .mkString("\n")
          logger.error(s"Authentication failure:\n$errorDetails")
          Failure(AuthenticationError("Authentication failure"))
      }
}
