package org.thp.scalligraph.controllers

import java.io.ByteArrayInputStream
import java.util.Date

import javax.inject.{Inject, Singleton}
import javax.naming.ldap.LdapName
import org.bouncycastle.asn1._
import org.thp.scalligraph.auth.{AuthContext, MultiAuthSrv, Permission, UserSrv}
import org.thp.scalligraph.{AuthenticationError, Instance}
import play.api.http.HeaderNames
import play.api.mvc._
import play.api.{Configuration, Logger}
import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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
  override def permissions: Seq[Permission] = authContext.permissions
  override def map[B](f: A ⇒ B): AuthenticatedRequest[B] =
    new AuthenticatedRequest(authContext, request.map(f))
}

object ExpirationStatus {
  sealed abstract class Type
  case class Ok(duration: FiniteDuration)      extends Type
  case class Warning(duration: FiniteDuration) extends Type
  case object Error                            extends Type
}

/**
  * Check and manager user security (authentication and authorization)
  *
  * @param maxSessionInactivity maximum time a session without any activity stay valid
  * @param sessionWarning
  * @param sessionUsername
  * @param userSrv
  * @param authSrv
  * @param defaultParser
  * @param ec
  */
@Singleton
class Authenticated(
    maxSessionInactivity: FiniteDuration,
    sessionWarning: FiniteDuration,
    sessionUsername: String,
    certificateField: Option[String],
    authBySessionCookie: Boolean,
    authByKey: Boolean,
    authByBasicAuth: Boolean,
    authByInitialUser: Boolean,
    authByPki: Boolean,
    userSrv: UserSrv,
    authSrv: MultiAuthSrv,
    defaultParser: BodyParsers.Default,
    implicit val ec: ExecutionContext) {

  @Inject() def this(
      configuration: Configuration,
      userSrv: UserSrv,
      authSrv: MultiAuthSrv,
      defaultParser: BodyParsers.Default,
      ec: ExecutionContext) =
    this(
      configuration.getMillis("session.inactivity").millis,
      configuration.getMillis("session.warning").millis,
      configuration.getOptional[String]("session.username").getOrElse("username"),
      configuration.getOptional[String]("auth.pki.certificateField"),
      configuration.getOptional[Boolean]("auth.method.session").getOrElse(true),
      configuration.getOptional[Boolean]("auth.method.key").getOrElse(true),
      configuration.getOptional[Boolean]("auth.method.basic").getOrElse(true),
      configuration.getOptional[Boolean]("auth.method.init").getOrElse(true),
      configuration.getOptional[Boolean]("auth.method.pki").getOrElse(true),
      userSrv,
      authSrv,
      defaultParser,
      ec
    )

  lazy val logger = Logger(getClass)

  private def now = (new Date).getTime

  /**
    * Insert or update session cookie containing user name and session expiration timestamp
    * Cookie is signed by Play framework (it cannot be modified by user)
    */
  def setSessingUser(result: Result, authContext: AuthContext)(implicit request: RequestHeader): Result =
    result.addingToSession(sessionUsername → authContext.userId, "expire" → (now + maxSessionInactivity.toMillis).toString)

  /**
    * Retrieve authentication information form cookie
    */
  def getFromSession(request: RequestHeader): Future[AuthContext] = {
    val userId = for {
      userId ← request.session.get(sessionUsername).toRight(AuthenticationError("User session not found"))
      _ = if (expirationStatus(request) != ExpirationStatus.Error) Right(()) else Left(AuthenticationError("User session has expired"))
    } yield userId
    userId match {
      case Right(uid)      ⇒ userSrv.getFromId(request, uid)
      case Left(authError) ⇒ Future.failed[AuthContext](authError)
    }
  }

  def expirationStatus(request: RequestHeader): ExpirationStatus.Type =
    request.session
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
  def getFromApiKey(request: RequestHeader): Future[AuthContext] =
    for {
      auth ← request.headers
        .get(HeaderNames.AUTHORIZATION)
        .fold(Future.failed[String](AuthenticationError("Authentication header not found")))(Future.successful)
      _ ← if (!auth.startsWith("Bearer ")) Future.failed(AuthenticationError("Only bearer authentication is supported")) else Future.successful(())
      key = auth.substring(7)
      authContext ← authSrv.authenticate(key)(request, ec)
    } yield authContext

  def getFromBasicAuth(request: RequestHeader): Future[AuthContext] =
    for {
      auth ← request.headers
        .get(HeaderNames.AUTHORIZATION)
        .fold(Future.failed[String](AuthenticationError("Authentication header not found")))(Future.successful)
      _ ← if (!auth.startsWith("Basic ")) Future.failed(AuthenticationError("Only basic authentication is supported")) else Future.successful(())
      authWithoutBasic = auth.substring(6)
      decodedAuth      = new String(java.util.Base64.getDecoder.decode(authWithoutBasic), "UTF-8")
      authContext ← decodedAuth.split(":") match {
        case Array(username, password) ⇒ authSrv.authenticate(username, password)(request, ec)
        case _                         ⇒ Future.failed(AuthenticationError("Can't decode authentication header"))
      }
    } yield authContext

  private def asn1String(obj: ASN1Primitive): String = obj match {
    case ds: DERUTF8String    ⇒ DERUTF8String.getInstance(ds).getString
    case to: ASN1TaggedObject ⇒ asn1String(ASN1TaggedObject.getInstance(to).getObject)
    case os: ASN1OctetString  ⇒ new String(os.getOctets)
    case as: ASN1String       ⇒ as.getString
  }

  private object CertificateSAN {
    def unapply(l: java.util.List[_]): Option[(String, String)] = {
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

  def getFromClientCertificate(request: RequestHeader): Future[AuthContext] =
    certificateField
      .fold[Future[AuthContext]](Future.failed(AuthenticationError("Certificate authentication is not configured"))) { cf ⇒
        request.clientCertificateChain
          .flatMap(_.headOption)
          .flatMap { cert ⇒
            val dn       = cert.getSubjectX500Principal.getName
            val ldapName = new LdapName(dn)
            ldapName.getRdns.asScala
              .collectFirst {
                case rdn if rdn.getType == cf ⇒ userSrv.getFromId(request, rdn.getValue.toString)
              }
              .orElse {
                for {
                  san ← Option(cert.getSubjectAlternativeNames)
                  fieldValue ← san.asScala.collectFirst {
                    case CertificateSAN(`cf`, value) ⇒ userSrv.getFromId(request, value)
                  }
                } yield fieldValue
              }
          }
          .getOrElse(Future.failed(AuthenticationError("Certificate doesn't contain user information")))
      }

  val authenticationMethods: Seq[(String, RequestHeader ⇒ Future[AuthContext])] =
    (if (authBySessionCookie) Seq("session" → getFromSession _) else Nil) ++
      (if (authByPki) Seq("pki"             → getFromClientCertificate _) else Nil) ++
      (if (authByKey) Seq("key"             → getFromApiKey _) else Nil) ++
      (if (authByBasicAuth) Seq("basic"     → getFromBasicAuth _) else Nil) ++
      (if (authByInitialUser) Seq("init"    → userSrv.getInitialUser _) else Nil)

  def getContext(request: RequestHeader): Future[AuthContext] =
    authenticationMethods
      .foldLeft[Future[Either[Seq[(String, Throwable)], AuthContext]]](Future.successful(Left(Nil))) {
        case (acc, (authMethodName, authMethod)) ⇒
          acc.flatMap {
            case authContext if authContext.isRight ⇒ Future.successful(authContext)
            case Left(errors) ⇒
              authMethod(request)
                .map(authContext ⇒ Right(authContext))
                .recover { case error ⇒ Left(errors :+ (authMethodName → error)) }
          }
      }
      .flatMap {
        case Right(authContext) ⇒ Future.successful(authContext)
        case Left(errors) ⇒
          val errorDetails = errors
            .map { case (authMethodName, error) ⇒ s"\t$authMethodName: ${error.getClass.getSimpleName} ${error.getMessage}" }
            .mkString("\n")
          logger.error(s"Authentication failure:\n$errorDetails")
          Future.failed(AuthenticationError("Authentication failure"))
      }
}
