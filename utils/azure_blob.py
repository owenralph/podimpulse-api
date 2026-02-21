import uuid
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import logging
from utils.constants import BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME
from typing import Optional, Union, List, Dict
import re
import hashlib
import json
from datetime import datetime, timezone

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
PODCAST_METADATA_PREFIX = "podcasts/"
PODCAST_INDEX_PREFIX = "indexes/podcasts/v1/"
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


class BlobDecodeError(ValueError):
    """Raised when blob bytes cannot be decoded as UTF-8."""


class PodcastIndexConflictError(ValueError):
    """Raised when attempting to create an index entry that already exists."""

    def __init__(self, index_name: str, value: str, existing_podcast_id: Optional[str] = None):
        self.index_name = index_name
        self.value = value
        self.existing_podcast_id = existing_podcast_id
        message = f"Index conflict for {index_name}='{value}'"
        if existing_podcast_id:
            message += f" (podcast_id={existing_podcast_id})"
        super().__init__(message)


def _is_uuid_like(value: str) -> bool:
    return bool(_UUID_RE.match(value or ""))


def _normalize_index_value(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _index_blob_name(index_name: str, value: str) -> str:
    normalized = _normalize_index_value(value)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"{PODCAST_INDEX_PREFIX}{index_name}/{digest}.json"


def _read_index_payload(index_name: str, value: str) -> Optional[Dict[str, str]]:
    blob_name = _index_blob_name(index_name, value)
    try:
        payload = _download_blob_by_name(blob_name, binary=False)
        return json.loads(payload)
    except ResourceNotFoundError:
        return None


def _download_blob_by_name(blob_name: str, binary: bool = False) -> Union[str, bytes]:
    blob_client = blob_container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob().readall()
    if binary:
        return blob_data
    try:
        return blob_data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise BlobDecodeError(f"Non-UTF8 blob payload: {e}") from e

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


def list_podcast_ids(include_legacy: bool = True) -> List[str]:
    """
    Lists podcast metadata IDs from the dedicated podcasts prefix and optionally
    legacy root blobs that look like UUID ids.
    """
    logging.debug(
        f"Listing podcast IDs from prefix={PODCAST_METADATA_PREFIX}, include_legacy={include_legacy}"
    )
    try:
        podcast_ids = set()

        for blob in blob_container_client.list_blobs(name_starts_with=PODCAST_METADATA_PREFIX):
            name = blob.name
            if not name.endswith(".json"):
                continue
            pid = name[len(PODCAST_METADATA_PREFIX):-5]
            if _is_uuid_like(pid):
                podcast_ids.add(pid)

        if include_legacy:
            for blob in blob_container_client.list_blobs():
                name = blob.name
                if "/" in name or not name.endswith(".json"):
                    continue
                pid = name[:-5]
                if _is_uuid_like(pid):
                    podcast_ids.add(pid)

        return sorted(podcast_ids)
    except Exception as e:
        logging.error(f"Error listing podcast IDs from Blob Storage: {e}")
        raise RuntimeError(f"Error listing podcast IDs from Blob Storage: {e}")


def save_podcast_blob(data: str, podcast_id: Optional[str] = None) -> str:
    """
    Saves podcast metadata/data in a dedicated prefix to isolate it from model artifacts.
    """
    logging.debug(f"Saving podcast blob. podcast_id={podcast_id}")
    try:
        podcast_id = podcast_id or str(uuid.uuid4())
        blob_name = f"{PODCAST_METADATA_PREFIX}{podcast_id}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Podcast dataset saved to Blob Storage with podcast_id: {podcast_id}")
        return podcast_id
    except Exception as e:
        logging.error(f"Error saving podcast blob to Blob Storage: {e}")
        raise RuntimeError(f"Error saving podcast blob to Blob Storage: {e}")


def load_podcast_blob(podcast_id: str, binary: bool = False) -> Union[str, bytes]:
    """
    Loads podcast metadata/data, preferring the dedicated prefix and falling back
    to legacy root location for backward compatibility.
    """
    logging.debug(f"Loading podcast blob. podcast_id={podcast_id}, binary={binary}")
    candidates = [
        f"{PODCAST_METADATA_PREFIX}{podcast_id}.json",
        f"{podcast_id}.json",
    ]

    not_found = 0
    last_error: Optional[Exception] = None
    for blob_name in candidates:
        try:
            result = _download_blob_by_name(blob_name, binary=binary)
            logging.info(f"Podcast dataset loaded from Blob Storage via blob_name: {blob_name}")
            return result
        except ResourceNotFoundError:
            not_found += 1
            continue
        except BlobDecodeError as e:
            logging.error(f"Non-retryable decode error loading podcast blob {blob_name}: {e}")
            raise
        except Exception as e:
            last_error = e
            logging.error(f"Error loading podcast blob {blob_name}: {e}")

    if not_found == len(candidates):
        raise RuntimeError(f"Podcast blob not found for podcast_id: {podcast_id}")
    raise RuntimeError(f"Error loading podcast blob for podcast_id {podcast_id}: {last_error}")


def delete_podcast_blob(podcast_id: str) -> str:
    """
    Deletes podcast metadata/data from prefixed storage and legacy fallback location.
    """
    logging.debug(f"Deleting podcast blob. podcast_id={podcast_id}")
    candidates = [
        f"{PODCAST_METADATA_PREFIX}{podcast_id}.json",
        f"{podcast_id}.json",
    ]
    deleted = False
    last_error: Optional[Exception] = None

    for blob_name in candidates:
        try:
            blob_client = blob_container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            deleted = True
        except ResourceNotFoundError:
            continue
        except Exception as e:
            last_error = e
            logging.error(f"Error deleting podcast blob {blob_name}: {e}")

    if deleted:
        logging.info(f"Podcast dataset deleted from Blob Storage with podcast_id: {podcast_id}")
        return podcast_id
    if last_error is not None:
        raise RuntimeError(f"Error deleting podcast blob for podcast_id {podcast_id}: {last_error}")
    raise RuntimeError(f"Podcast blob not found for podcast_id: {podcast_id}")


def get_podcast_id_from_index(index_name: str, value: str) -> Optional[str]:
    payload = _read_index_payload(index_name, value)
    if not payload:
        return None
    return payload.get("podcast_id")


def create_podcast_index(
    index_name: str,
    value: str,
    podcast_id: str,
    overwrite: bool = False,
) -> None:
    """
    Creates (or overwrites) an index entry for podcast uniqueness checks.
    """
    try:
        normalized = _normalize_index_value(value)
        blob_name = _index_blob_name(index_name, value)
        payload = {
            "podcast_id": podcast_id,
            "index_name": index_name,
            "value": value,
            "normalized_value": normalized,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(payload), overwrite=overwrite)
    except ResourceExistsError:
        existing = _read_index_payload(index_name, value) or {}
        raise PodcastIndexConflictError(
            index_name=index_name,
            value=value,
            existing_podcast_id=existing.get("podcast_id"),
        )
    except PodcastIndexConflictError:
        raise
    except Exception as e:
        logging.error(f"Error creating podcast index {index_name}: {e}")
        raise RuntimeError(f"Error creating podcast index {index_name}: {e}")


def delete_podcast_index(
    index_name: str,
    value: str,
    expected_podcast_id: Optional[str] = None,
) -> bool:
    """
    Deletes an index entry. If expected_podcast_id is provided, deletes only when it matches.
    """
    blob_name = _index_blob_name(index_name, value)
    try:
        if expected_podcast_id:
            existing = _read_index_payload(index_name, value)
            if not existing:
                return False
            if existing.get("podcast_id") != expected_podcast_id:
                return False
        blob_client = blob_container_client.get_blob_client(blob_name)
        blob_client.delete_blob()
        return True
    except ResourceNotFoundError:
        return False
    except Exception as e:
        logging.error(f"Error deleting podcast index {index_name}: {e}")
        raise RuntimeError(f"Error deleting podcast index {index_name}: {e}")


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
        blob_data = _download_blob_by_name(blob_name, binary=binary)
        logging.info(f"Dataset loaded from Blob Storage with instance_id: {instance_id}")
        return blob_data
    except BlobDecodeError as e:
        logging.error(f"Non-retryable decode error loading from Blob Storage: {e}")
        raise
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
