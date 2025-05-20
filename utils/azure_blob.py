import uuid
from azure.storage.blob import BlobServiceClient
import logging
from utils.constants import BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME
from typing import Optional

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

def save_to_blob_storage(data: str, instance_id: Optional[str] = None) -> str:
    """
    Saves JSON data to Azure Blob Storage. If an instance_id is provided, updates the existing blob.
    Otherwise, creates a new blob and returns a unique instance_id.
    Args:
        data (str): JSON string to save.
        instance_id (Optional[str]): Optional instance ID for the blob.
    Returns:
        str: The instance_id used for the blob.
    Raises:
        RuntimeError: If saving to blob fails.
    """
    try:
        instance_id = instance_id or str(uuid.uuid4())
        blob_name = f"{instance_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Dataset saved to Blob Storage with instance_id: {instance_id}")
        return instance_id
    except Exception as e:
        raise RuntimeError(f"Error saving to Blob Storage: {e}")

def load_from_blob_storage(instance_id: str) -> str:
    """
    Loads JSON data from Azure Blob Storage using an instance_id.
    Args:
        instance_id (str): The instance ID for the blob.
    Returns:
        str: The JSON string loaded from the blob.
    Raises:
        RuntimeError: If loading from blob fails.
    """
    try:
        blob_name = f"{instance_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        logging.info(f"Dataset loaded from Blob Storage with instance_id: {instance_id}")
        return blob_data.decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Error loading from Blob Storage: {e}")
