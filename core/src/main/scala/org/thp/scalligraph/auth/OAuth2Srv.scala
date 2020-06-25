package org.thp.scalligraph.auth

import java.util.UUID

import com.google.inject.Provider
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.controllers.AuthenticatedRequest
import org.thp.scalligraph.{AuthenticationError, BadConfigurationError, BadRequestError, CreateError, NotFoundError}
import play.api.libs.json.JsObject
import play.api.libs.ws.WSClient
import play.api.mvc._
import play.api.{Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrantType extends Enumeration {
  val authorizationCode: Value = Value("authorization_code")
  // Only this supported atm
}

object ResponseType extends Enumeration {
  val code: Value = Value("code")
  // Only this supported atm
}

case class OAuth2Config(
    clientId: String,
    clientSecret: String,
    redirectUri: String,
    responseType: ResponseType.Value,
    grantType: GrantType.Value,
    authorizationUrl: String,
    tokenUrl: String,
    userUrl: String,
    scope: Seq[String],
    userIdField: String,
    userOrganisationField: Option[String],
    defaultOrganisation: Option[String],
    authorizationHeader: String
)

class TokenizedRequest[A](val token: Option[String], request: Request[A])        extends WrappedRequest[A](request)
class OAuthenticatedRequest[A](val user: JsObject, request: TokenizedRequest[A]) extends WrappedRequest[A](request)

class OAuth2Srv(
    httpContext: String,
    OAuth2Config: OAuth2Config,
    userSrv: UserSrv,
    WSClient: WSClient,
    sessionAuthProvider: Provider[AuthSrv]
)(
    implicit ec: ExecutionContext
) extends AuthSrv {
  lazy val logger: Logger          = Logger(getClass)
  lazy val sessionAuthSrv: AuthSrv = sessionAuthProvider.get()
  val name: String                 = "oauth2"
  val endpoint: String             = "/ssoLogin"

  override def capabilities: Set[AuthCapability.Value] = super.capabilities ++ Set(AuthCapability.sso)

  /**
    * Main Auth action
    * @return
    */
  override def actionFunction(nextFunction: ActionFunction[Request, AuthenticatedRequest]): ActionFunction[Request, AuthenticatedRequest] =
    new ActionFunction[Request, AuthenticatedRequest] {
      override def invokeBlock[A](request: Request[A], block: AuthenticatedRequest[A] => Future[Result]): Future[Result] =
        if (isSSO(request)) {
          logger.debug("Found SSO request")
          OAuth2Config.grantType match {
            case GrantType.authorizationCode =>
              if (!isSecuredAuthCode(request)) {
                logger.debug("Code or state is not provided, redirect to authorizationUrl")
                Future.successful(authRedirect())
              } else {
                for {
                  tokenizedRequest <- authTokenFromCode(request)
                  oauth2Req        <- userFromToken(tokenizedRequest)
                  authReq <- Future
                    .fromTry(authenticate(oauth2Req))
                    .recoverWith {
                      case _: NotFoundError => Future.fromTry(createUser(oauth2Req))
                    }
                    .recoverWith {
                      case _: CreateError => Future.failed(NotFoundError("User not found"))
                    }
                } yield sessionAuthSrv.setSessionUser(authReq.authContext)(Results.Found(httpContext))
              }

            case x => Future.failed(BadConfigurationError(s"OAuth GrantType $x not supported yet"))
          }
        } else nextFunction.invokeBlock(request, block)

      override protected def executionContext: ExecutionContext = ec
    }

  def isSSO(request: Request[_]): Boolean = request.path.endsWith(endpoint)

  def isSecuredAuthCode(request: Request[_]): Boolean =
    request.queryString.contains(ResponseType.code.toString) && request.queryString.contains("state")

  /**
    * Filter checking whether we initiate the OAuth2 process
    * and redirecting to OAuth2 server if necessary
    * @return
    */
  private def authRedirect(): Result = {
    val state = UUID.randomUUID().toString
    val queryStringParams = Map[String, Seq[String]](
      "scope"         -> Seq(OAuth2Config.scope.mkString(" ")),
      "response_type" -> Seq(ResponseType.code.toString),
      "redirect_uri"  -> Seq(OAuth2Config.redirectUri),
      "client_id"     -> Seq(OAuth2Config.clientId),
      "state"         -> Seq(state)
    )

    logger.debug(s"Redirecting to ${OAuth2Config.redirectUri} with $queryStringParams and state $state")
    Results
      .Redirect(OAuth2Config.authorizationUrl, queryStringParams, status = 302)
      .withSession("state" -> state)
  }

  /**
    * Enriching the initial request with OAuth2 token gotten
    * from OAuth2 code
    * @return
    */
  private def authTokenFromCode[A](request: Request[A]): Future[TokenizedRequest[A]] =
    if (!isSSO(request)) {
      Future.successful(new TokenizedRequest[A](None, request))
    } else {
      {
        for {
          state   <- request.session.get("state")
          stateQs <- request.queryString.get("state").flatMap(_.headOption)
          if state == stateQs
        } yield {
          request.queryString.get(ResponseType.code.toString) match {
            case Some(code) =>
              logger.debug(s"Attempting to retrieve OAuth2 token from ${OAuth2Config.tokenUrl} with code $code")
              getAuthTokenFromCode(code.head, state)
                .map { t =>
                  logger.trace(s"Got token $t")
                  new TokenizedRequest[A](Some(t), request)
                }
            case None =>
              Future.failed(AuthenticationError(s"OAuth2 server code missing ${request.queryString.get("error")}"))
          }
        }
      } getOrElse Future.failed(BadRequestError("OAuth2 states mismatch"))
    }

  /**
    * Querying the OAuth2 server for a token
    * @param code the previously obtained code
    * @return
    */
  private def getAuthTokenFromCode(code: String, state: String): Future[String] = {
    logger.trace(s"""
                    |Request to ${OAuth2Config.tokenUrl} with
                    |  code:          $code
                    |  grant_type:    ${OAuth2Config.grantType}
                    |  client_secret: ${OAuth2Config.clientSecret}
                    |  redirect_uri:  ${OAuth2Config.redirectUri}
                    |  client_id:     ${OAuth2Config.clientId}
                    |  state:         $state
                    |""".stripMargin)
    WSClient
      .url(OAuth2Config.tokenUrl)
      .withHttpHeaders("Accept" -> "application/json")
      .post(
        Map(
          "code"          -> code,
          "grant_type"    -> OAuth2Config.grantType.toString,
          "client_secret" -> OAuth2Config.clientSecret,
          "redirect_uri"  -> OAuth2Config.redirectUri,
          "client_id"     -> OAuth2Config.clientId,
          "state"         -> state
        )
      )
      .transform {
        case Success(r) if r.status == 200 => Success((r.json \ "access_token").asOpt[String].getOrElse(""))
        case Failure(error)                => Failure(AuthenticationError(s"OAuth2 token verification failure ${error.getMessage}"))
        case Success(r)                    => Failure(AuthenticationError(s"OAuth2/token unexpected response from server (${r.status} ${r.statusText})"))
      }
  }

  /**
    * Enriched action with OAuth2 server user data
    * @return
    */
  private def userFromToken[A](request: TokenizedRequest[A]): Future[OAuthenticatedRequest[A]] =
    request
      .token
      .fold[Future[JsObject]](Future.failed(AuthenticationError("Token is missing")))(getUserDataFromToken)
      .map { userJson =>
        logger.debug(s"Got user info: $userJson")
        new OAuthenticatedRequest[A](userJson, request)
      }

  /**
    * Client query for user data with OAuth2 token
    * @param token the token
    * @return
    */
  private def getUserDataFromToken(token: String): Future[JsObject] = {
    logger.trace(s"Request to ${OAuth2Config.userUrl} with authorization header: ${OAuth2Config.authorizationHeader} $token")
    WSClient
      .url(OAuth2Config.userUrl)
      .addHttpHeaders("Authorization" -> s"${OAuth2Config.authorizationHeader} $token")
      .get()
      .transform {
        case Success(r) if r.status == 200 => Success(r.json.as[JsObject])
        case Failure(error)                => Failure(AuthenticationError(s"OAuth2 user data fetch failure ${error.getMessage}"))
        case Success(r)                    => Failure(AuthenticationError(s"OAuth2/userinfo unexpected response from server (${r.status} ${r.statusText})"))
      }
  }

  private def authenticate[A](request: OAuthenticatedRequest[A]): Try[AuthenticatedRequest[A]] =
    for {
      userId      <- getUserId(request.user)
      authContext <- userSrv.getAuthContext(request, userId, getUserOrganisation(request.user))
    } yield new AuthenticatedRequest[A](authContext, request)

  private def getUserOrganisation(jsonUser: JsObject): Option[String] =
    OAuth2Config
      .userOrganisationField
      .flatMap(orgField => (jsonUser \ orgField).asOpt[String])
      .orElse(OAuth2Config.defaultOrganisation)

  private def getUserId(jsonUser: JsObject): Try[String] =
    (jsonUser \ OAuth2Config.userIdField).asOpt[String] match {
      case Some(userId) => Success(userId)
      case None         => Failure(BadRequestError(s"OAuth2 user data doesn't contain user ID field (${OAuth2Config.userIdField})"))
    }

  private def createUser[A](request: OAuthenticatedRequest[A]): Try[AuthenticatedRequest[A]] =
    for {
      userId      <- getUserId(request.user)
      createdUser <- userSrv.createUser(userId, request.user)
      authContext <- userSrv.getAuthContext(request, createdUser.id, None)
    } yield new AuthenticatedRequest[A](authContext, request)
}

@Singleton
class OAuth2Provider @Inject() (
    userSrv: UserSrv,
    WSClient: WSClient,
    implicit val executionContext: ExecutionContext,
    provider: Provider[AuthSrv],
    globalConfig: Configuration
) extends AuthSrvProvider {
  override val name: String = "oauth2"
  override def apply(configuration: Configuration): Try[AuthSrv] =
    for {
      clientId            <- configuration.getOrFail[String]("clientId")
      clientSecret        <- configuration.getOrFail[String]("clientSecret")
      redirectUri         <- configuration.getOrFail[String]("redirectUri")
      responseType        <- configuration.getOrFail[String]("responseType").flatMap(rt => Try(ResponseType.withName(rt)))
      grantType           <- configuration.getOrFail[String]("grantType").flatMap(gt => Try(GrantType.withName(gt)))
      authorizationUrl    <- configuration.getOrFail[String]("authorizationUrl")
      userUrl             <- configuration.getOrFail[String]("userUrl")
      tokenUrl            <- configuration.getOrFail[String]("tokenUrl")
      scope               <- configuration.getOrFail[Seq[String]]("scope")
      userIdField         <- configuration.getOrFail[String]("userIdField")
      authorizationHeader <- configuration.getOrFail[String]("authorizationHeader")
      userOrganisationField = configuration.getOptional[String]("organisationField")
      defaultOrganisation   = configuration.getOptional[String]("defaultOrganisation")
    } yield new OAuth2Srv(
      globalConfig.get[String]("play.http.context"),
      OAuth2Config(
        clientId,
        clientSecret,
        redirectUri,
        responseType,
        grantType,
        authorizationUrl,
        tokenUrl,
        userUrl,
        scope,
        userIdField,
        userOrganisationField,
        defaultOrganisation,
        authorizationHeader
      ),
      userSrv,
      WSClient,
      provider
    )
}
