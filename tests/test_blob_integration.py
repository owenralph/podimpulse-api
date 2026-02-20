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


if __name__ == "__main__":
    unittest.main()
