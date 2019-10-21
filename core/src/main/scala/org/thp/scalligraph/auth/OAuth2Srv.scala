package org.thp.scalligraph.auth

import com.google.inject.Provider
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.auth.GrantType.GrantType
import org.thp.scalligraph.auth.ResponseType.ResponseType
import org.thp.scalligraph.controllers.AuthenticatedRequest
import org.thp.scalligraph.{AuthenticationError, BadConfigurationError}
import play.api.libs.json.JsObject
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc._
import play.api.{Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object GrantType extends Enumeration {
  type GrantType = Value

  val authorizationCode: GrantType = Value("authorization_code")
  // Only this supported atm
}

object ResponseType extends Enumeration {
  type ResponseType = Value

  val code: ResponseType = Value("code")
  // Only this supported atm
}

case class OAuth2Config(
    clientId: String,
    clientSecret: String,
    redirectUri: String,
    responseType: ResponseType,
    grantType: GrantType,
    authorizationUrl: String,
    tokenUrl: String,
    userUrl: String,
    scope: Seq[String],
    autoCreate: Boolean,
    autoUpdate: Boolean
)

class TokenizedRequest[A](val token: Option[String], request: Request[A])                extends WrappedRequest[A](request)
class OAuthenticatedRequest[A](val user: Option[JsObject], request: TokenizedRequest[A]) extends WrappedRequest[A](request)

class OAuth2Srv(
    OAuth2Config: OAuth2Config,
    userSrv: UserSrv,
    WSClient: WSClient,
    configuration: Configuration,
    sessionAuthProvider: Provider[AuthSrv]
)(
    implicit ec: ExecutionContext
) extends AuthSrv {
  lazy val logger                  = Logger(getClass)
  lazy val sessionAuthSrv: AuthSrv = sessionAuthProvider.get()
  val name: String                 = "oauth2"
  val endpoint: String             = "ssoLogin"

  /**
    * Main Auth action
    * @return
    */
  override def actionFunction(nextFunction: ActionFunction[Request, AuthenticatedRequest]): ActionFunction[Request, AuthenticatedRequest] =
    new ActionFunction[Request, AuthenticatedRequest] {
      override def invokeBlock[A](request: Request[A], block: AuthenticatedRequest[A] => Future[Result]): Future[Result] =
        if (request.uri.contains(endpoint)) {
          OAuth2Config.grantType match {
            case GrantType.authorizationCode =>
              if (!request.queryString.contains(ResponseType.code.toString)) {
                authRedirect(request)
              } else {
                for {
                  tokenizedRequest <- authTokenFromCode(request)
                  oauth2Req        <- userFromToken(tokenizedRequest)
                  authReq          <- authenticate(oauth2Req)
                } yield sessionAuthSrv.setSessionUser(authReq.authContext)(Results.Ok)
              }

            case x => Future.failed(BadConfigurationError(s"OAuth GrantType $x not supported yet"))
          }
        } else nextFunction.invokeBlock(request, block)

      override protected def executionContext: ExecutionContext = ec
    }

  /**
    * Filter checking whether we initiate the OAuth2 process
    * and redirecting to OAuth2 server if necessary
    * @return
    */
  private def authRedirect[A](input: Request[A]) = {
    val queryStringParams = Map[String, Seq[String]](
      "scope"         -> Seq(OAuth2Config.scope.mkString(" ")),
      "response_type" -> Seq(ResponseType.code.toString),
      "redirect_uri"  -> Seq(OAuth2Config.redirectUri),
      "client_id"     -> Seq(OAuth2Config.clientId)
    )

    logger.debug(s"Redirecting to ${OAuth2Config.redirectUri} with $queryStringParams")
    Future.successful(Results.Redirect(OAuth2Config.authorizationUrl, queryStringParams, status = 200))
  }

  /**
    * Enriching the initial request with OAuth2 token gotten
    * from OAuth2 code
    * @return
    */
  private def authTokenFromCode[A](request: Request[A]) =
    if (!request.uri.contains(endpoint)) {
      Future.successful(new TokenizedRequest[A](None, request))
    } else if (!request.queryString.contains(ResponseType.code.toString)) {
      Future.failed(AuthenticationError(s"OAuth2 server code missing ${request.queryString.get("error")}"))
    } else {
      val code = request.queryString(ResponseType.code.toString).headOption.getOrElse("")

      logger.debug(s"Attempting to retrieve OAuth2 token from ${OAuth2Config.tokenUrl} with code $code")
      getAuthTokenFromCode(code)
        .map(t => new TokenizedRequest[A](Some(t), request))
    }

  /**
    * Querying the OAuth2 server for a token
    * @param code the previously obtained code
    * @return
    */
  private def getAuthTokenFromCode(code: String): Future[String] =
    WSClient
      .url(OAuth2Config.tokenUrl)
      .post(
        Map(
          "code"          -> code,
          "grant_type"    -> OAuth2Config.grantType.toString,
          "client_secret" -> OAuth2Config.clientSecret,
          "redirect_uri"  -> OAuth2Config.redirectUri,
          "client_id"     -> OAuth2Config.clientId
        )
      )
      .recoverWith {
        case error => Future.failed(AuthenticationError(s"OAuth2 token verification failure ${error.getMessage}"))
      }
      .flatMap {
        case r: WSResponse if r.status == 200 => Future.successful((r.json \ "access_token").asOpt[String].getOrElse(""))
        case _                                => Future.failed(AuthenticationError("OAuth2 unexpected response from server"))
      }

  /**
    * Enriched action with OAuth2 server user data
    * @return
    */
  private def userFromToken[A](request: TokenizedRequest[A]) = {
    for {
      token    <- Future.fromTry(Try(request.token.get))
      userJson <- getUserDataFromToken(token)
    } yield new OAuthenticatedRequest[A](Some(userJson), request)
  } recoverWith {
    case e =>
      if (request.token.isDefined)
        logger.error(s"OAuth2 failed to fetch user data ${e.getMessage} with token ${request.token} at ${OAuth2Config.userUrl}")

      Future.successful(new OAuthenticatedRequest[A](None, request))
  }

  /**
    * Client query for user data with OAuth2 token
    * @param token the token
    * @return
    */
  private def getUserDataFromToken(token: String): Future[JsObject] =
    WSClient
      .url(OAuth2Config.userUrl)
      .addHttpHeaders("Authorization" -> s"Bearer $token")
      .get()
      .recoverWith {
        case error => Future.failed(AuthenticationError(s"OAuth2 user data fetch failure ${error.getMessage}"))
      }
      .flatMap {
        case r: WSResponse if r.status == 200 => Future.successful(r.json.as[JsObject])

        case _ => Future.failed(AuthenticationError("OAuth2 user data fetch unexpected response from server"))
      }

  private def authenticate[A](request: OAuthenticatedRequest[A]) =
    for {
      jsonUser <- Future.fromTry(Try(request.user.get))
      user = SimpleUser.apply(jsonUser, configuration)
      authContext <- Future.fromTry(userSrv.getAuthContext(request, user.id, user.organisation))
    } yield new AuthenticatedRequest[A](authContext, request)

  case class SimpleUser(id: String, name: String, organisation: Option[String]) extends User {
    override def getUserName: String = name
  }

  object SimpleUser {

    def apply(jsObject: JsObject, configuration: Configuration): SimpleUser = {
      val idField           = configuration.getOptional[String]("auth.sso.attributes.userId").getOrElse("")
      val nameField         = configuration.getOptional[String]("auth.sso.attributes.userName").getOrElse("")
      val organisationField = configuration.getOptional[String]("auth.sso.attributes.organisation").getOrElse("")

      SimpleUser(
        (jsObject \ idField).asOpt[String].getOrElse(""),
        (jsObject \ nameField).asOpt[String].getOrElse("noname"),
        (jsObject \ organisationField).asOpt[String].orElse(configuration.getOptional[String]("auth.sso.defaultOrganisation"))
      )
    }
  }
}

@Singleton
class OAuth2Provider @Inject()(
    userSrv: UserSrv,
    config: Configuration,
    WSClient: WSClient,
    implicit val executionContext: ExecutionContext,
    provider: Provider[AuthSrv]
) extends AuthSrvProvider {
  override val name: String = "oauth2"
  override def apply(configuration: Configuration): Try[AuthSrv] =
    for {
      clientId         <- configuration.getOrFail[String]("clientId")
      clientSecret     <- configuration.getOrFail[String]("clientSecret")
      redirectUri      <- configuration.getOrFail[String]("redirectUri")
      responseType     <- configuration.getOrFail[String]("responseType").flatMap(rt => Try(ResponseType.withName(rt)))
      grantType        <- configuration.getOrFail[String]("grantType").flatMap(gt => Try(GrantType.withName(gt)))
      authorizationUrl <- configuration.getOrFail[String]("authorizationUrl")
      userUrl          <- configuration.getOrFail[String]("userUrl")
      tokenUrl         <- configuration.getOrFail[String]("tokenUrl")
      scope            <- configuration.getOrFail[Seq[String]]("scope")
      autoCreate = config.getOptional[Boolean]("auth.sso.autocreate").getOrElse(false)
      autoUpdate = config.getOptional[Boolean]("auth.sso.autoupdate").getOrElse(false)
    } yield new OAuth2Srv(
      OAuth2Config(clientId, clientSecret, redirectUri, responseType, grantType, authorizationUrl, tokenUrl, userUrl, scope, autoCreate, autoUpdate),
      userSrv,
      WSClient,
      config,
      provider
    )
}
