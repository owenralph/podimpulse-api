import importlib
import json
import os
import socket
import unittest
import uuid

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


class FakeRequest:
    def __init__(self, method="GET", route_params=None, json_body=None, params=None, headers=None):
        self.method = method
        self.route_params = route_params or {}
        self.params = params or {}
        self.headers = headers or {}
        self._json_body = json_body
        self.files = {}
        self.form = {}

    def get_json(self):
        if self._json_body is None:
            raise ValueError("Invalid JSON body")
        return self._json_body


class BlobAzuriteIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["BLOB_CONNECTION_STRING"] = AZURITE_CONNECTION_STRING
        cls.container_name = "podcast-data"

        sock = socket.socket()
        sock.settimeout(1)
        try:
            sock.connect(("127.0.0.1", 10000))
        except Exception as exc:
            raise unittest.SkipTest(f"Azurite is not reachable: {exc}")
        finally:
            sock.close()

        try:
            service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
            container_client = service_client.get_container_client(cls.container_name)
            try:
                container_client.create_container()
            except ResourceExistsError:
                pass
        except Exception as exc:
            raise unittest.SkipTest(f"Azurite is not reachable: {exc}")

        import utils.constants as constants
        import utils.azure_blob as azure_blob

        importlib.reload(constants)
        cls.azure_blob = importlib.reload(azure_blob)
        import functions.v1.initialize as initialize_module
        cls.initialize_module = importlib.reload(initialize_module)

    def test_blob_json_roundtrip_list_and_delete(self):
        payload = {"message": "hello", "value": 123}
        instance_id = f"itest-{uuid.uuid4().hex}"

        saved_id = self.azure_blob.save_to_blob_storage(json.dumps(payload), instance_id)
        self.assertEqual(saved_id, instance_id)

        loaded = self.azure_blob.load_from_blob_storage(instance_id)
        self.assertEqual(json.loads(loaded), payload)

        all_ids = self.azure_blob.list_all_blob_ids()
        self.assertIn(instance_id, all_ids)

        deleted_id = self.azure_blob.delete_blob_from_storage(instance_id)
        self.assertEqual(deleted_id, instance_id)

        with self.assertRaises(RuntimeError):
            self.azure_blob.load_from_blob_storage(instance_id)

    def test_blob_binary_roundtrip(self):
        payload = b"\x00\x01joblib-bytes"
        instance_id = f"itest-bin-{uuid.uuid4().hex}"

        saved_id = self.azure_blob.save_to_blob_storage(payload, instance_id)
        self.assertEqual(saved_id, instance_id)

        loaded_bytes = self.azure_blob.load_from_blob_storage(instance_id, binary=True)
        self.assertEqual(loaded_bytes, payload)

        self.azure_blob.delete_blob_from_storage(instance_id)

    def test_load_from_blob_storage_non_utf8_raises_value_error(self):
        payload = b"\x80\x81binary"
        instance_id = f"itest-decode-{uuid.uuid4().hex}"
        try:
            self.azure_blob.save_to_blob_storage(payload, instance_id)
            with self.assertRaises(ValueError):
                self.azure_blob.load_from_blob_storage(instance_id)
        finally:
            try:
                self.azure_blob.delete_blob_from_storage(instance_id)
            except Exception:
                pass

    def test_podcast_blob_listing_filters_non_podcast_artifacts(self):
        podcast_id = str(uuid.uuid4())
        artifact_id = f"{podcast_id}_ridge_model.joblib"
        payload = {"title": "Pod", "rss_url": "https://example.com/feed.xml"}

        try:
            self.azure_blob.save_podcast_blob(json.dumps(payload), podcast_id)
            self.azure_blob.save_to_blob_storage(b"\x00\x01artifact", artifact_id)

            podcast_ids = self.azure_blob.list_podcast_ids(include_legacy=True)
            self.assertIn(podcast_id, podcast_ids)
            self.assertNotIn(artifact_id, podcast_ids)
        finally:
            try:
                self.azure_blob.delete_podcast_blob(podcast_id)
            except Exception:
                pass
            try:
                self.azure_blob.delete_blob_from_storage(artifact_id)
            except Exception:
                pass

    def test_initialize_handler_roundtrip_against_azurite(self):
        title = f"Azurite Podcast {uuid.uuid4().hex[:8]}"
        rss_url = "https://example.com/feed.xml"
        podcast_id = None
        try:
            create_req = FakeRequest(
                method="POST",
                json_body={"title": title, "rss_url": rss_url},
            )
            create_resp = self.initialize_module.initialize(create_req)
            self.assertEqual(create_resp.status_code, 201)
            create_body = json.loads(create_resp.get_body().decode("utf-8"))
            podcast_id = create_body["result"]["podcast_id"]

            list_req = FakeRequest(method="GET")
            list_resp = self.initialize_module.initialize(list_req)
            self.assertEqual(list_resp.status_code, 200)
            list_body = json.loads(list_resp.get_body().decode("utf-8"))
            listed_ids = {item["podcast_id"] for item in list_body["result"]}
            self.assertIn(podcast_id, listed_ids)

            get_req = FakeRequest(method="GET", route_params={"podcast_id": podcast_id})
            get_resp = self.initialize_module.podcast_resource(get_req)
            self.assertEqual(get_resp.status_code, 200)
            get_body = json.loads(get_resp.get_body().decode("utf-8"))
            self.assertEqual(get_body["result"]["title"], title)
            self.assertEqual(get_body["result"]["rss_url"], rss_url)
        finally:
            if podcast_id:
                delete_req = FakeRequest(method="DELETE", route_params={"podcast_id": podcast_id})
                self.initialize_module.podcast_resource(delete_req)


if __name__ == "__main__":
    unittest.main()
