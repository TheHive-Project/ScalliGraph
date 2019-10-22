package org.thp.scalligraph.auth

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import play.api.libs.json.JsObject
import play.api.libs.ws.WSClient
import play.api.mvc._
import play.api.{Configuration, Logger}

import com.google.inject.Provider
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph.controllers.AuthenticatedRequest
import org.thp.scalligraph.{AuthenticationError, BadConfigurationError, BadRequestError}

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
    defaultOrganisation: Option[String]
)

class TokenizedRequest[A](val token: Option[String], request: Request[A])        extends WrappedRequest[A](request)
class OAuthenticatedRequest[A](val user: JsObject, request: TokenizedRequest[A]) extends WrappedRequest[A](request)

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
  val endpoint: String             = "/ssoLogin"

  def isSSO(request: Request[_]): Boolean = request.path.endsWith(endpoint)

  /**
    * Main Auth action
    * @return
    */
  override def actionFunction(nextFunction: ActionFunction[Request, AuthenticatedRequest]): ActionFunction[Request, AuthenticatedRequest] =
    new ActionFunction[Request, AuthenticatedRequest] {
      override def invokeBlock[A](request: Request[A], block: AuthenticatedRequest[A] => Future[Result]): Future[Result] =
        if (isSSO(request)) {
          OAuth2Config.grantType match {
            case GrantType.authorizationCode =>
              if (!request.queryString.contains(ResponseType.code.toString)) {
                authRedirect(request)
              } else {
                for {
                  tokenizedRequest <- authTokenFromCode(request)
                  oauth2Req        <- userFromToken(tokenizedRequest)
                  authReq          <- Future.fromTry(authenticate(oauth2Req))
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
  private def authRedirect[A](input: Request[A]): Future[Result] = {
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
  private def authTokenFromCode[A](request: Request[A]): Future[TokenizedRequest[A]] =
    if (!isSSO(request)) {
      Future.successful(new TokenizedRequest[A](None, request))
    } else {
      request.queryString.get(ResponseType.code.toString) match {
        case Some(code) =>
          logger.debug(s"Attempting to retrieve OAuth2 token from ${OAuth2Config.tokenUrl} with code $code")
          getAuthTokenFromCode(code.head)
            .map(t => new TokenizedRequest[A](Some(t), request))
        case None =>
          Future.failed(AuthenticationError(s"OAuth2 server code missing ${request.queryString.get("error")}"))
      }
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
      .transform {
        case Success(r) if r.status == 200 => Success((r.json \ "access_token").asOpt[String].getOrElse(""))
        case Failure(error)                => Failure(AuthenticationError(s"OAuth2 token verification failure ${error.getMessage}"))
        case Success(r)                    => Failure(AuthenticationError(s"OAuth2 unexpected response from server (${r.status} ${r.statusText})"))
      }

  /**
    * Enriched action with OAuth2 server user data
    * @return
    */
  private def userFromToken[A](request: TokenizedRequest[A]): Future[OAuthenticatedRequest[A]] =
    for {
      token    <- Future.fromTry(Try(request.token.get))
      userJson <- getUserDataFromToken(token)
    } yield new OAuthenticatedRequest[A](userJson, request)

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
      .transform {
        case Success(r) if r.status == 200 => Success(r.json.as[JsObject])
        case Failure(error)                => Failure(AuthenticationError(s"OAuth2 user data fetch failure ${error.getMessage}"))
        case Success(r)                    => Failure(AuthenticationError(s"OAuth2 unexpected response from server (${r.status} ${r.statusText})"))
      }

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

  private def getUserId(jsonUser: JsObject): Try[String] =
    (jsonUser \ OAuth2Config.userIdField).asOpt[String] match {
      case Some(userId) => Success(userId)
      case None         => Failure(BadRequestError(s"OAuth2 user data doesn't contain user ID field (${OAuth2Config.userIdField})"))
    }

  private def getUserOrganisation(jsonUser: JsObject): Option[String] =
    OAuth2Config
      .userOrganisationField
      .flatMap(orgField => (jsonUser \ orgField).asOpt[String])
      .orElse(OAuth2Config.defaultOrganisation)

  private def authenticate[A](request: OAuthenticatedRequest[A]): Try[AuthenticatedRequest[A]] =
    for {
      userId      <- getUserId(request.user)
      authContext <- userSrv.getAuthContext(request, userId, getUserOrganisation(request.user))
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
      userIdField      <- configuration.getOrFail[String]("userIdField")
      userOrganisationField = configuration.getOptional[String]("userOrganisation")
      defaultOrganisation   = configuration.getOptional[String]("defaultOrganisation")
    } yield new OAuth2Srv(
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
        defaultOrganisation
      ),
      userSrv,
      WSClient,
      config,
      provider
    )
}
