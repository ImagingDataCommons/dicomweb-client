import time
import unittest

import responses  # type: ignore
import requests

from dicomweb_client.session_utils import (
    ConfidentialClientCredentials,
    ConfidentialClientSession,
    create_session_from_client_credentials,
    PublicClientCredentials,
)


class TestConfidentialClientCredentials(unittest.TestCase):

    def test_construction(self) -> None:
        client_id = "test"
        client_secret = "client_secret"
        token_uri = "https://test.mghpathology.org/token"
        credentials = ConfidentialClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
            token_uri=token_uri
        )
        self.assertIsInstance(credentials, ConfidentialClientCredentials)
        self.assertEqual(credentials.client_id, client_id)
        self.assertEqual(credentials.client_secret, client_secret)
        self.assertEqual(credentials.token_uri, token_uri)

    def test_construction_missing_client_secret(self) -> None:
        with self.assertRaises(TypeError):
            ConfidentialClientCredentials(
                client_id="test",
                token_uri="https://test.mghpathology.org/token"
            )

    def test_construction_extra_auth_uri(self) -> None:
        with self.assertRaises(TypeError):
            ConfidentialClientCredentials(
                client_id="id",
                client_secret="secret",
                token_uri="https://test.mghpathology.org/token",
                auth_uri="https://test.mghpathology.org/auth",
            )


class TestPublicClientCredentials(unittest.TestCase):

    def test_construction(self) -> None:
        client_id = "test"
        token_uri = "https://test.mghpathology.org/token"
        auth_uri = "https://test.mghpathology.org/auth"
        credentials = PublicClientCredentials(
            client_id=client_id,
            token_uri=token_uri,
            auth_uri=auth_uri,
        )
        self.assertIsInstance(credentials, PublicClientCredentials)
        self.assertEqual(credentials.client_id, client_id)
        self.assertEqual(credentials.token_uri, token_uri)
        self.assertEqual(credentials.auth_uri, auth_uri)

    def test_construction_extra_client_secret(self) -> None:
        with self.assertRaises(TypeError):
            PublicClientCredentials(
                client_id="id",
                client_secret="secret",
                token_uri="https://test.mghpathology.org/token",
            )

    def test_construction_missing_auth_uri(self) -> None:
        with self.assertRaises(TypeError):
            PublicClientCredentials(
                client_id="id",
                token_uri="https://test.mghpathology.org/token",
            )


class TestConfidentialClientSession(unittest.TestCase):
    def setUp(self) -> None:
        self._credentials = ConfidentialClientCredentials(
            client_id="test",
            client_secret="test",
            token_uri="https://test.mghpathology.org/token",
        )
        self._token = {
            "token_type": "Bearer",
            "access_token": "a",
            "refresh_token": "b",
            "expires_in": "1",
            "expires_at": str(time.time()),
        }

    @responses.activate
    def test_construction(self) -> None:
        responses.add(
            responses.Response(
                method="POST",
                url=self._credentials.token_uri,
                json=self._token,
                status=200,
            )
        )
        session = ConfidentialClientSession(
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            token_uri=self._credentials.token_uri,
        )
        self.assertIsInstance(session, ConfidentialClientSession)
        self.assertIsInstance(session, requests.Session)
        self.assertIsInstance(session.token, dict)
        self.assertEqual(
            session.token["access_token"],
            self._token["access_token"]
        )

    @responses.activate
    def test_construction_using_factory_function(self) -> None:
        responses.add(
            responses.Response(
                method="POST",
                url=self._credentials.token_uri,
                json=self._token,
                status=200,
            )
        )
        session = create_session_from_client_credentials(self._credentials)
        self.assertIsInstance(session, ConfidentialClientSession)
        self.assertIsInstance(session, requests.Session)

    @responses.activate
    def test_construction_with_scope(self) -> None:
        responses.add(
            responses.Response(
                method="POST",
                url=self._credentials.token_uri,
                json=self._token,
                status=200,
            )
        )
        scope = [
            "foo",
            "bar",
        ]
        ConfidentialClientSession(
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            token_uri=self._credentials.token_uri,
            scope=scope,
        )
