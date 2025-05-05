import logging
import os
from typing import Optional, Any
import warnings

import requests

logger = logging.getLogger(__name__)


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
