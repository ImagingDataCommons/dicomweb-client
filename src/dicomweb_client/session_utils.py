import logging
import os
from typing import Optional

import requests
from google.auth import credentials

logger = logging.getLogger(__name__)


def create_session_from_auth(
        auth: [requests.auth.AuthBase]) -> requests.Session:
    '''
    Parameters
    ----------
    auth: requests.auth.AuthBase, optional
        an implementation of `requests.auth.AuthBase` to be used for
        authentication with services

    Returns
    -------
    requests.Session
        authenticated session

    '''
    session = requests.Session()
    session.auth = auth
    return session


def create_session_from_user_pass(username: [str],
                                  password: [str]) -> requests.Session:
    '''
    Parameters
    ----------
    username: str,
        username for authentication with services
    password: str,
        password for authentication with services

    Returns
    -------
    requests.Session
        authenticated session

    '''
    session = requests.Session()
    session.auth = (username, password)
    return session


def create_verified_session(ca_bundle: Optional[str] = None,
                            cert: Optional[str] = None) -> requests.Session:
    '''
    Parameters
    ----------
    ca_bundle: str, optional
        path to CA bundle file
    cert: str, optional
        path to client certificate file in Privacy Enhanced Mail (PEM) format

    Returns
    -------
    requests.Session
        verified session

    '''
    if ca_bundle is None and cert is None:
        raise ValueError(
            'Both ca_bundle and cert cannot be None'
        )

    session = requests.Session()
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
        google_credentials: [credentials]) -> requests.Session:
    '''
    Parameters
    ----------
    google_credentials: google.auth.credentials
        Google cloud credentials.
        (See https://cloud.google.com/docs/authentication/production
        for more information on Google cloud authentication)

    Returns
    -------
    requests.Session
        Google cloud authorized session

    '''
    from google.auth.transport import requests as google_requests
    return google_requests.AuthorizedSession(google_credentials)
