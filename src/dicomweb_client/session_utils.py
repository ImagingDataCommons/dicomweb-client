import base64
import hashlib
import logging
import os
import random
import re
import string
import time
import warnings
import webbrowser
from abc import ABCMeta
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, Callable, List, NamedTuple, NoReturn, Optional, Union

import requests
from oauthlib.oauth2 import (  # type: ignore
    Client as OAuth2Client,
    BackendApplicationClient,
    WebApplicationClient,
)
from requests_oauthlib.oauth2_session import OAuth2Session  # type: ignore


logger = logging.getLogger(__name__)


_STORE = {}


class _AuthorizationCodeError(Exception):
    """Exception raised when an authorization code could not be obtained.

    An authorization code is obtained from the authorization server as part of
    the OAuth 2.0 Authorization Code grant type.

    """

    pass


class _LocalHTTPRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler.

    Handles received redirected HTTP request messages from the authorization
    server as part of the OAuth 2.0 Authorization Code flow.

    """
    def _extract_value(self, param: str) -> str:
        pattern = re.compile(rf"[^_]{param}=([^&]+)")
        match = pattern.search(self.path)
        if match is None:
            raise ValueError(f'Value of parameter "{param}" not found in URI.')
        return match.group(1)

    def do_GET(self) -> None:
        """Respond to GET request."""
        try:
            code = self._extract_value("code")
            state = self._extract_value("state")
            _STORE[state] = code
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            page = b"""
                <html>
                    <head>
                        <title>Local Server</title>
                        <link rel="icon" href="data:;base64,iVBORw0KGgo=">
                    </head>
                    <body>
                        Success
                    </body>
                </html>
            """
            self.wfile.write(page)
        except ValueError:
            self.send_response(401)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            page = b"""
                <html>
                    <head>
                        <title>Local Server</title>
                        <link rel="icon" href="data:;base64,iVBORw0KGgo=">
                    </head>
                    <body>
                        Unauthorized
                    </body>
                </html>
            """
            self.wfile.write(page)


class _LocalHTTPServer(Thread):
    """Local HTTP server running in a thread.

    Receives redirected HTTP request messages from the authorization server as
    part of the OAuth 2.0 Authorization Code flow.

    """

    def __init__(self, port: int):
        """Construct object.

        Parameters
        ----------
        port: int
            Port on localhost to which server should listen

        """
        super().__init__()
        address = ("127.0.0.1", port)
        self.server = HTTPServer(address, _LocalHTTPRequestHandler)
        self.daemon = False

    def __enter__(self) -> '_LocalHTTPServer':
        """Enter scope of with statement block."""
        logger.debug("start local server")
        self.start()
        return self

    def __exit__(
        self,
        error_type: Optional[type],
        error_value: Optional[str],
        error_traceback: Optional[object]
    ) -> None:
        """Exit scope of with statement block.

        Parameters
        ----------
        except_type: type, optional
            Error class
        except_value: str, optional
            Error message
        except_trace: types.TracebackType, optional
            Error traceback

        """
        if error_type is not None:
            logger.error(f"an error occured: {error_value}")
        logger.debug("stop local server")
        self.stop()
        if error_type is not None:
            raise error_type(error_value)

    def run(self) -> None:
        """Start serving."""
        self.server.serve_forever()

    def stop(self) -> None:
        """Shut down the server."""
        self.server.shutdown()
        self.join()
        self.server.server_close()
        self.server.socket.close()

    def get_authorization_code(self, state: str) -> str:
        """Get a cached authorization code.

        Parameters
        ----------
        state: str
            Value of the OAuth 2.0 "state" parameter

        Returns
        -------
        str
            Authorization code

        """
        logger.debug("check if authorization code has been received")
        try:
            return _STORE[state]
        except KeyError:
            raise _AuthorizationCodeError(f'Code not found for state "{state}"')


class _AuthorizedSession(OAuth2Session, metaclass=ABCMeta):
    """Abstract base class for an authorized OAuth 2.0 session."""

    def __init__(
        self,
        client: OAuth2Client,
        scope: Optional[List[str]] = None,
        token_updater: Optional[Callable] = None,
        auto_refresh_url: Optional[str] = None,
        auto_refresh_kwargs: Optional[dict] = None,
        redirect_uri: Optional[str] = None,
    ) -> None:
        """Construct object.

        Parameters
        ----------
        client: oauthlib.oauth2.Client
            OAuth 2.0 Client object
        scope: List[str], optional
            Restricted scope of client access
        token_updater: Callable, optional
            Function for handling retrieved access tokens
            (signature: ``def token_updater(token: Dict[str, str]) -> None``)
        auto_refresh_url: str, optional
            URL for automatically refreshing  access tokens
        auto_refresh_kwargs: dict, optional
            Parameters for automatically refreshing access tokens
        redirect_uri: str, optional
            URI of service to which authorization requests will be redirected
            to

        """
        super().__init__(
            client=client,
            scope=scope,
            token_updater=token_updater,
            redirect_uri=redirect_uri,
            auto_refresh_url=auto_refresh_url,
            auto_refresh_kwargs=auto_refresh_kwargs,
        )


class PublicClientCredentials(NamedTuple):
    """Credentials for a public OAuth 2.0 client."""

    client_id: str
    token_uri: str
    auth_uri: str


class ConfidentialClientCredentials(NamedTuple):
    """Credentials for a confidential OAuth 2.0 client."""

    client_id: str
    client_secret: str
    token_uri: str


class PublicClientSession(_AuthorizedSession):
    """Authorized session for public OAuth 2.0 clients.

    Should be used by clients that are incapable of maintaining the
    confidentiality of their credentials. For example, a shell in an
    environment that is under the control of the resource owner.

    Uses the OAuth 2.0 Authorization Code grant type with
    Proof Key for Code Exchange (PKCE) challenge.

    """

    def __init__(
        self,
        client_id: str,
        auth_uri: str,
        token_uri: str,
        token_updater: Optional[Callable] = None,
        scope: Optional[List[str]] = None,
        redirect_port: int = 37474,
        redirect_timeout: int = 30,
        open_browser: bool = True
    ) -> None:
        """Construct object.

        Parameters
        ----------
        client_id: str
            Identifier of an OAuth 2.0 client.
        auth_uri: str
            Unique resource identifier of the authorization endpoint - used by
            the client to obtain authorization via redirection
        token_uri: str
            Unique resource identifier of the token endpoint - used by the
            client to obtain an access token via an authorization code grant
        token_updater: Callable, optional
            Function for handling retrieved access tokens
            (signature: ``def token_updater(token: Dict[str, str]) -> None``)
        scope: List[str], optional
            Restricted scope of client access
        redirect_port: int, optional
            Local port of HTTP server to which authentication requests will
            be redirected
        redirect_timeout: int, optional
            Seconds to wait for redirect message
        open_browser: bool, optional
            Whether the authorization URL should automatically be opened in
            a new tab of the default browser

        Note
        ----
        When `open_browser` is set to ``True``, a window should open
        in your browser and prompt you for your credentials. Otherwise, the
        authorization URL must be obtained from the log message and copied
        manually into a browser.

        Note
        ----
        The OAuth 2.0 client must be configured to authorize the redirect URI
        (default: ``"http://localhost:37474/"``). The URI must match exactly,
        including the final slash.

        """
        logger.info(
            "create session for public client using the authentication code "
            "grant type"
        )

        redirect_server = _LocalHTTPServer(redirect_port)
        redirect_uri = f"http://localhost:{redirect_port}/"

        client = WebApplicationClient(client_id=client_id, token={})
        super().__init__(
            client=client,
            scope=scope,
            token_updater=token_updater,
            redirect_uri=redirect_uri,
            auto_refresh_url=token_uri,
            auto_refresh_kwargs={"client_id": client_id},
        )

        code_verifier = self._create_code_verifier()
        extra_query_params = {
            "code_challenge": self._create_s256_code_challenge(code_verifier),
            "code_challenge_method": "S256",
        }
        extra_query_string = "&".join(
            [f"{key}={value}" for key, value in extra_query_params.items()]
        )

        logger.info("authenticate via web application")
        with redirect_server as redirect_session:
            authorization_url, state = self.authorization_url(auth_uri)
            logger.info(f'authorization URL: "{authorization_url}"')
            if open_browser:
                webbrowser.open_new_tab(authorization_url)

            logger.info("wait for receipt of authorization code")
            code = None
            t_end = time.time() + redirect_timeout
            while time.time() < t_end:
                try:
                    code = redirect_session.get_authorization_code(state)
                    break
                except _AuthorizationCodeError:
                    time.sleep(1)
                    continue
            if not code:
                raise ValueError("Could not obtain authorization code.")

            authorization_url += f"&{extra_query_string}"
            logger.info("fetch access token using received authorization code")
            self.fetch_token(
                token_url=token_uri,
                authorization_response=authorization_url,
                code_verifier=code_verifier,
                code=code,
            )

    @staticmethod
    def _create_code_verifier() -> str:
        """Create a code verifier for PKCE code challenge.

        Returns
        -------
        str
            Code verifier

        """
        chars = string.ascii_letters + string.digits
        rand = random.SystemRandom()
        return "".join(rand.choice(chars) for _ in range(48))

    @staticmethod
    def _create_s256_code_challenge(value: Union[str, int, float]) -> str:
        """Create a PKCE code challenge using the S256 method.

        Parameters
        ----------
        code_verifier: Union[int, float, str]
            Random string of 48 random characters

        Returns
        -------
        str
            Code challenge

        """
        # Encodes the provided values as follows: BASE64(SHA256(ASCII(value)))
        data = hashlib.sha256(bytes(str(value).encode("ascii"))).digest()
        data = base64.urlsafe_b64encode(data).rstrip(b"=")
        return data.decode("utf-8")


class ConfidentialClientSession(_AuthorizedSession):
    """Authorized session for confidential OAuth 2.0 clients.

    Should be used for clients that are capable of maintaining the
    confidentiality of their credentials. For example, a client used by an
    application server on the backend in a secure environment.

    Uses the OAuth 2.0 Client Credentials grant type.

    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_uri: str,
        scope: Optional[List[str]] = None,
        token_updater: Optional[Callable] = None,
    ):
        """Construct object.

        Parameters
        ----------
        client_id: str
            Client identifier
        client_secret: str
            Client secret
        token_uri: str
            Unique resource identifier of the token endpoint - used by the
            client to obtain an access token via the provided client secret
        token_updater: Callable, optional
            Function for handling retrieved access tokens
            (signature: ``def token_updater(token: Dict[str, str]) -> None``)
        scope: List[str], optional
            Restricted scope of client access

        """
        logger.info(
            "create session for confidential client using the client "
            "authentication grant type"
        )
        client = BackendApplicationClient(client_id=client_id)
        super().__init__(
            client=client,
            scope=scope,
            auto_refresh_url=token_uri,
            token_updater=token_updater,
            auto_refresh_kwargs={
                "client_id": client_id,
                "client_secret": client_secret,
            },
        )
        logger.info("fetch access token using client credentials")
        self.fetch_token(client_secret=client_secret, token_url=token_uri)


def create_session_from_client_credentials(
    credentials: Union[ConfidentialClientCredentials, PublicClientCredentials]
) -> requests.Session:
    """Construct an authorized session for accessing protected web resources.

    Parameters
    ----------
    credentials: Union[dicomweb_client.session_utils.ConfidentialClientCredentials, dicomweb_client.session_utils.PublicClientCredentials]
        Credentials of OAuth 2.0 client (public or confidential)

    Returns
    -------
    requests.Session
        Authorized session object

    """  # noqa
    if isinstance(credentials, PublicClientCredentials):
        return PublicClientSession(
            client_id=credentials.client_id,
            auth_uri=credentials.auth_uri,
            token_uri=credentials.token_uri,
        )
    elif isinstance(credentials, ConfidentialClientCredentials):
        return ConfidentialClientSession(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            token_uri=credentials.token_uri,
        )
    else:
        raise TypeError(
            'Argument "credentials" must be of type '
            '"PublicClientCredentials" or "ConfidentialClientCredentials".'
        )


def create_session() -> requests.Session:
    """Creates an unauthorized session.

    Returns
    -------
    requests.Session
        unauthorized session

    """
    logger.debug('initialize HTTP session')
    return requests.Session()


def create_session_from_auth(
    auth: requests.auth.AuthBase
) -> requests.Session:
    """Creates a session from a gicen AuthBase object.

    Parameters
    ----------
    auth: requests.auth.AuthBase
        an implementation of `requests.auth.AuthBase` to be used for
        authentication with services

    Returns
    -------
    requests.Session
        authorized session

    """
    logger.debug('initialize HTTP session')
    session = requests.Session()
    logger.debug('authenticate HTTP session')
    session.auth = auth
    return session


def create_session_from_user_pass(
    username: str,
    password: str
) -> requests.Session:
    """Creates a session from a given username and password.

    Parameters
    ----------
    username: str
        username for authentication with services
    password: str
        password for authentication with services

    Returns
    -------
    requests.Session
        authorized session

    """
    logger.debug('initialize HTTP session')
    session = requests.Session()
    logger.debug('authenticate and authorize HTTP session')
    session.auth = (username, password)
    return session


def add_certs_to_session(
    session: requests.Session,
    ca_bundle: Optional[str] = None,
    cert: Optional[str] = None
) -> requests.Session:
    """Adds CA bundle and certificate to an existing session.

    Parameters
    ----------
    session: requests.Session
        input session
    ca_bundle: str, optional
        path to CA bundle file
    cert: str, optional
        path to client certificate file in Privacy Enhanced Mail (PEM) format

    Returns
    -------
    requests.Session
        verified session

    """
    if ca_bundle is not None:
        ca_bundle = os.path.expanduser(os.path.expandvars(ca_bundle))
        if not os.path.exists(ca_bundle):
            raise OSError(
                'CA bundle file does not exist: {}'.format(ca_bundle)
            )
        logger.debug('use CA bundle file: {}'.format(ca_bundle))
        session.verify = ca_bundle
    if cert is not None:
        cert = os.path.expanduser(os.path.expandvars(cert))
        if not os.path.exists(cert):
            raise OSError(
                'Certificate file does not exist: {}'.format(cert)
            )
        logger.debug('use certificate file: {}'.format(cert))
        session.cert = cert
    return session


def create_session_from_gcp_credentials(
    google_credentials: Optional[Any] = None
) -> requests.Session:
    """Creates an authorized session for Google Cloud Platform.

    Parameters
    ----------
    google_credentials: Any
        Google cloud credentials.
        (see https://cloud.google.com/docs/authentication/production
        for more information on Google cloud authentication).
        If not set, will be initialized to ``google.auth.default()``

    Returns
    -------
    requests.Session
        Google cloud authorized session
    """
    warnings.warn(
        'This method shall be deprecated in a future release. Prefer using the '
        'underlying implementation directly, now moved to '
        '`dicomweb_client.ext.gcp.session_utils`.',
        DeprecationWarning)
    import dicomweb_client.ext.gcp.session_utils as gcp_session_utils
    return gcp_session_utils.create_session_from_gcp_credentials(
        google_credentials)
