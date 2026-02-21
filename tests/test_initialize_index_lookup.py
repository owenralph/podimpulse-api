import importlib
import json
import os
import unittest
from unittest.mock import patch

os.environ["BLOB_CONNECTION_STRING"] = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=testaccount;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "EndpointSuffix=core.windows.net"
)

from utils.azure_blob import PodcastIndexConflictError

initialize_module = importlib.import_module("functions.v1.initialize")


class FakeRequest:
    def __init__(self, method="POST", route_params=None, json_body=None):
        self.method = method
        self.route_params = route_params or {}
        self._json_body = json_body
        self.params = {}
        self.headers = {}

    def get_json(self):
        if self._json_body is None:
            raise ValueError("Invalid JSON body")
        return self._json_body


def _no_retry(func, exceptions, max_attempts=3, initial_delay=1.0, backoff_factor=2.0, logger=None, operation_name=None):
    del exceptions, max_attempts, initial_delay, backoff_factor, logger, operation_name
    return lambda *args, **kwargs: func(*args, **kwargs)


class InitializeIndexLookupTests(unittest.TestCase):
    @patch("functions.v1.initialize.retry_with_backoff", side_effect=_no_retry)
    @patch("functions.v1.initialize.get_podcast_id_from_index", return_value="existing-podcast")
    @patch("functions.v1.initialize.create_podcast_index")
    def test_create_conflict_from_index_returns_409(
        self,
        mock_create_index,
        _mock_get_index,
        _mock_retry,
    ):
        req = FakeRequest(
            method="POST",
            json_body={"title": "My Show", "rss_url": "https://example.com/feed.xml"},
        )
        resp = initialize_module.initialize(req)

        self.assertEqual(resp.status_code, 409)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertIn("already exists", body["message"])
        mock_create_index.assert_not_called()

    @patch("functions.v1.initialize.retry_with_backoff", side_effect=_no_retry)
    @patch("functions.v1.initialize.get_podcast_id_from_index", return_value=None)
    @patch("functions.v1.initialize.save_podcast_blob")
    @patch("functions.v1.initialize.create_podcast_index")
    @patch("functions.v1.initialize.uuid.uuid4")
    def test_create_uses_indexes_and_saves_podcast_blob(
        self,
        mock_uuid4,
        mock_create_index,
        mock_save_blob,
        _mock_get_index,
        _mock_retry,
    ):
        mock_uuid4.return_value = "11111111-1111-1111-1111-111111111111"
        mock_save_blob.return_value = str(mock_uuid4.return_value)

        req = FakeRequest(
            method="POST",
            json_body={"title": "My Show", "rss_url": "https://example.com/feed.xml"},
        )
        resp = initialize_module.initialize(req)

        self.assertEqual(resp.status_code, 201)
        body = json.loads(resp.get_body().decode("utf-8"))
        self.assertEqual(body["result"]["podcast_id"], str(mock_uuid4.return_value))

        mock_create_index.assert_any_call("title", "My Show", str(mock_uuid4.return_value), overwrite=False)
        mock_create_index.assert_any_call("rss", "https://example.com/feed.xml", str(mock_uuid4.return_value), overwrite=False)
        mock_save_blob.assert_called_once()

    @patch("functions.v1.initialize.retry_with_backoff", side_effect=_no_retry)
    @patch("functions.v1.initialize.get_podcast_id_from_index", return_value=None)
    @patch("functions.v1.initialize.delete_podcast_index")
    @patch("functions.v1.initialize.save_podcast_blob", side_effect=RuntimeError("save failed"))
    @patch("functions.v1.initialize.create_podcast_index")
    @patch("functions.v1.initialize.uuid.uuid4")
    def test_create_rolls_back_reserved_indexes_when_save_fails(
        self,
        mock_uuid4,
        _mock_create_index,
        _mock_save_blob,
        mock_delete_index,
        _mock_get_index,
        _mock_retry,
    ):
        mock_uuid4.return_value = "22222222-2222-2222-2222-222222222222"

        req = FakeRequest(
            method="POST",
            json_body={"title": "Show 2", "rss_url": "https://example.com/feed2.xml"},
        )
        resp = initialize_module.initialize(req)

        self.assertEqual(resp.status_code, 500)
        mock_delete_index.assert_any_call("title", "Show 2", expected_podcast_id=str(mock_uuid4.return_value))
        mock_delete_index.assert_any_call("rss", "https://example.com/feed2.xml", expected_podcast_id=str(mock_uuid4.return_value))

    @patch("functions.v1.initialize.retry_with_backoff", side_effect=_no_retry)
    @patch("functions.v1.initialize.load_podcast_blob", return_value=json.dumps({"title": "Old", "rss_url": "https://example.com/old.xml"}))
    @patch("functions.v1.initialize.create_podcast_index", side_effect=PodcastIndexConflictError("title", "New"))
    @patch("functions.v1.initialize.save_podcast_blob")
    def test_patch_conflict_on_index_reservation_returns_409(
        self,
        mock_save_blob,
        _mock_create_index,
        _mock_load_blob,
        _mock_retry,
    ):
        req = FakeRequest(
            method="PATCH",
            route_params={"podcast_id": "pod-1"},
            json_body={"title": "New"},
        )
        resp = initialize_module.podcast_resource(req)
        self.assertEqual(resp.status_code, 409)
        mock_save_blob.assert_not_called()

    @patch("functions.v1.initialize.retry_with_backoff", side_effect=_no_retry)
    @patch("functions.v1.initialize.load_podcast_blob", return_value=json.dumps({"title": "Old", "rss_url": "https://example.com/old.xml"}))
    @patch("functions.v1.initialize.delete_podcast_blob", return_value="pod-1")
    @patch("functions.v1.initialize.delete_podcast_index")
    def test_delete_removes_indexes_after_blob_delete(
        self,
        mock_delete_index,
        _mock_delete_blob,
        _mock_load_blob,
        _mock_retry,
    ):
        req = FakeRequest(method="DELETE", route_params={"podcast_id": "pod-1"})
        resp = initialize_module.podcast_resource(req)

        self.assertEqual(resp.status_code, 200)
        mock_delete_index.assert_any_call("title", "Old", expected_podcast_id="pod-1")
        mock_delete_index.assert_any_call("rss", "https://example.com/old.xml", expected_podcast_id="pod-1")


if __name__ == "__main__":
    unittest.main()
