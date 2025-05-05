"""Session management utilities for Google Cloud Platform (GCP)."""
from typing import Optional, Any

try:
    import google.auth
    from google.auth.transport import requests as google_requests
except ImportError:
    raise ImportError(
        'The `dicomweb-client` package needs to be installed with the '
        '"gcp" extra requirements to use this module, as follows: '
        '`pip install dicomweb-client[gcp]`')
import requests


def create_session_from_gcp_credentials(
        google_credentials: Optional[Any] = None
    ) -> requests.Session:
    """Creates an authorized session for Google Cloud Platform.

    Parameters
    ----------
    google_credentials: Any
        Google Cloud credentials.
        (see https://cloud.google.com/docs/authentication/production
        for more information on Google Cloud authentication).
        If not set, will be initialized to ``google.auth.default()``.

    Returns
    -------
    requests.Session
        Google Cloud authorized session.

    Note
    ----
    Credentials will be read from environment variable
    ``GOOGLE_APPLICATION_CREDENTIALS`` if set.

    """
    if google_credentials is None:
        google_credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
    return google_requests.AuthorizedSession(google_credentials)
