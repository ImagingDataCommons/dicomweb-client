"""Session management utilities for Google Cloud Platform (GCP)."""
from typing import Optional, Any

import google.auth
from google.auth.transport import requests as google_requests
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
    """
    if google_credentials is None:
        google_credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
    return google_requests.AuthorizedSession(google_credentials)
