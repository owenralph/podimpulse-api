import json
import os
import unittest
from unittest.mock import patch


# Keep test imports resilient if other modules initialize blob clients.
os.environ.setdefault(
    "BLOB_CONNECTION_STRING",
    (
        "DefaultEndpointsProtocol=https;"
        "AccountName=testaccount;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "EndpointSuffix=core.windows.net"
    ),
)

from functions.v1.tiktok import accounts as accounts_module  # noqa: E402
from functions.v1.tiktok import analytics as analytics_module  # noqa: E402
from functions.v1.tiktok import token as token_module  # noqa: E402


class FakeRequest:
    def __init__(self, method="POST", json_body=None):
        self.method = method
        self._json_body = json_body

    def get_json(self):
        if self._json_body is None:
            raise ValueError("Invalid JSON body")
        return self._json_body


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class TikTokHandlerTests(unittest.TestCase):
    def test_exchange_user_token_success(self):
        req = FakeRequest(json_body={"auth_code": "auth-123", "redirect_uri": "https://example.com/callback"})
        fake_api_response = FakeResponse(
            {
                "access_token": "tt-access",
                "refresh_token": "tt-refresh",
                "open_id": "acct-1",
                "scope": "user.info.basic",
                "expires_in": 3600,
            }
        )

        with patch.object(token_module, "TIKTOK_CLIENT_KEY", "client-key"), patch.object(
            token_module, "TIKTOK_CLIENT_SECRET", "client-secret"
        ), patch("functions.v1.tiktok.token.requests.post", return_value=fake_api_response):
            resp = token_module.exchange_user_token(req)

        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertEqual(body["access_token"], "tt-access")
        self.assertEqual(body["open_id"], "acct-1")

    def test_get_user_accounts_success(self):
        req = FakeRequest(json_body={"user_token": "tt-user-token"})
        fake_api_response = FakeResponse(
            {"data": {"user": {"open_id": "acct-42", "display_name": "Creator 42"}}}
        )

        with patch("functions.v1.tiktok.accounts.requests.get", return_value=fake_api_response):
            resp = accounts_module.get_user_accounts(req)

        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertEqual(body["accounts"], [{"id": "acct-42", "name": "Creator 42"}])

    def test_get_account_token_mismatch_returns_403(self):
        req = FakeRequest(json_body={"user_token": "tt-user-token", "account_id": "acct-99"})
        fake_api_response = FakeResponse(
            {"data": {"user": {"open_id": "acct-42", "display_name": "Creator 42"}}}
        )

        with patch("functions.v1.tiktok.token.requests.get", return_value=fake_api_response):
            resp = token_module.get_account_token(req)

        self.assertEqual(resp.status_code, 403)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertIn("does not match", body["message"])

    def test_query_video_analytics_success(self):
        req = FakeRequest(json_body={"account_token": "tt-account-token", "max_count": 5})
        fake_api_response = FakeResponse(
            {
                "data": {
                    "videos": [
                        {
                            "id": "vid-1",
                            "title": "Post title",
                            "video_description": "Post description",
                            "create_time": 1735689600,
                            "view_count": 1200,
                            "like_count": 90,
                            "comment_count": 11,
                            "share_count": 7,
                        }
                    ]
                }
            }
        )

        with patch("functions.v1.tiktok.analytics.requests.post", return_value=fake_api_response):
            resp = analytics_module.query_video_analytics(req)

        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertEqual(body["status"], "success")
        self.assertEqual(len(body["videos"]), 1)
        self.assertEqual(body["videos"][0]["id"], "vid-1")
        self.assertEqual(body["videos"][0]["insights"]["view_count"], 1200)


if __name__ == "__main__":
    unittest.main()
