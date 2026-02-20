import uuid
from azure.storage.blob import BlobServiceClient
import logging
from utils.constants import BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME
from typing import Optional, Union, List

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
    logging.debug(f"Saving data to blob storage. instance_id={instance_id}")
    try:
        instance_id = instance_id or str(uuid.uuid4())
        blob_name = f"{instance_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Dataset saved to Blob Storage with instance_id: {instance_id}")
        return instance_id
    except Exception as e:
        logging.error(f"Error saving to Blob Storage: {e}")
        raise RuntimeError(f"Error saving to Blob Storage: {e}")

def list_all_blob_ids() -> List[str]:
    """
    Lists all blob instance IDs in the container, stripping the .json suffix.

    Returns:
        List[str]: Blob instance IDs.

    Raises:
        RuntimeError: If listing blobs fails.
    """
    logging.debug("Listing all blob IDs from blob storage.")
    try:
        blob_ids: List[str] = []
        for blob in blob_container_client.list_blobs():
            name = blob.name
            if name.endswith(".json"):
                blob_ids.append(name[:-5])
        return blob_ids
    except Exception as e:
        logging.error(f"Error listing blobs from Blob Storage: {e}")
        raise RuntimeError(f"Error listing blobs from Blob Storage: {e}")

def load_from_blob_storage(instance_id: str, binary: bool = False) -> Union[str, bytes]:
    """
    Loads data from Azure Blob Storage using an instance_id.

    Args:
        instance_id (str): The instance ID for the blob.
        binary (bool): If True, returns raw bytes. Otherwise, returns decoded utf-8 string.

    Returns:
        Union[str, bytes]: The blob data as a string or bytes.

    Raises:
        RuntimeError: If loading from blob fails.
    """
    logging.debug(f"Loading data from blob storage. instance_id={instance_id}, binary={binary}")
    try:
        blob_name = f"{instance_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        logging.info(f"Dataset loaded from Blob Storage with instance_id: {instance_id}")
        if binary:
            return blob_data
        return blob_data.decode("utf-8")
    except Exception as e:
        logging.error(f"Error loading from Blob Storage: {e}")
        raise RuntimeError(f"Error loading from Blob Storage: {e}")

def delete_blob_from_storage(instance_id: str) -> str:
    """
    Deletes a blob from Azure Blob Storage using an instance_id.

    Args:
        instance_id (str): The instance ID for the blob.

    Returns:
        str: The deleted instance_id.

    Raises:
        RuntimeError: If deleting from blob fails.
    """
    logging.debug(f"Deleting data from blob storage. instance_id={instance_id}")
    try:
        blob_name = f"{instance_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.delete_blob()
        logging.info(f"Dataset deleted from Blob Storage with instance_id: {instance_id}")
        return instance_id
    except Exception as e:
        logging.error(f"Error deleting from Blob Storage: {e}")
        raise RuntimeError(f"Error deleting from Blob Storage: {e}")
