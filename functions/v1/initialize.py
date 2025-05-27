import azure.functions as func
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import logging
import json
import time
from typing import Optional
from utils.retry import retry_with_backoff
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage

def initialize(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to initialize a new instance and create an empty blob.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the new instance ID or error message.
    """
    logging.debug("[initialize] Received request to initialize a new instance.")
    start_time = time.time()

    # Validate HTTP method
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    # Create an empty JSON object
    empty_json = "{}"

    # Save the empty JSON to Azure Blob Storage with retry
    save_start = time.time()
    instance_id, err = handle_blob_operation(
        retry_with_backoff(lambda: save_to_blob_storage(empty_json), exceptions=(RuntimeError,), max_attempts=3, initial_delay=1.0, backoff_factor=2.0)
    )
    if err:
        return error_response("Failed to initialize instance.", 500)
    save_duration = time.time() - save_start
    logging.info(f"Blob save completed in {save_duration:.2f} seconds.")

    # Return the instance ID in a JSON response
    response_data = {
        "message": "Instance initialized successfully.",
        "result": {"instance_id": instance_id}
    }
    total_duration = time.time() - start_time
    logging.info(f"Total function execution time: {total_duration:.2f} seconds.")
    return json_response(response_data, 200)

def podcast_resource(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handles GET, PUT, PATCH, DELETE for /v1/podcasts/{podcast_id}, including rss_url as a property.
    """
    method_error = validate_http_method(req, ["GET", "PUT", "PATCH", "DELETE"])
    if method_error:
        return method_error

    podcast_id = req.route_params.get("podcast_id")
    if not podcast_id:
        return error_response("Missing podcast_id in path.", 400)

    if req.method == "GET":
        try:
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load podcast data.", 404)
            json_data = json.loads(blob_data)
            if not json_data.get("title") or not json_data.get("rss_url"):
                return error_response("Podcast metadata incomplete.", 404)
            return json_response({
                "message": "Podcast retrieved successfully.",
                "result": {
                    "podcast_id": podcast_id,
                    "title": json_data["title"],
                    "rss_url": json_data["rss_url"]
                }
            }, 200)
        except Exception as e:
            logging.error(f"Failed to retrieve podcast: {e}", exc_info=True)
            return error_response("Failed to retrieve podcast.", 500)

    elif req.method in ("PUT", "PATCH"):
        try:
            request_data = req.get_json()
            title = request_data.get("title")
            rss_url = request_data.get("rss_url")
            if req.method == "PUT":
                if not title or not rss_url:
                    return error_response("Missing title or rss_url.", 400)
                json_data = {"title": title, "rss_url": rss_url}
            else:  # PATCH
                blob_data, err = handle_blob_operation(
                    retry_with_backoff(
                        lambda: load_from_blob_storage(podcast_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )
                )
                if err:
                    return error_response("Failed to load podcast data.", 404)
                json_data = json.loads(blob_data)
                if title:
                    json_data["title"] = title
                if rss_url:
                    json_data["rss_url"] = rss_url
            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: save_to_blob_storage(json.dumps(json_data), podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to save podcast data.", 500)
            return json_response({
                "message": "Podcast updated successfully.",
                "result": {
                    "podcast_id": podcast_id,
                    "title": json_data.get("title"),
                    "rss_url": json_data.get("rss_url")
                }
            }, 200)
        except Exception as e:
            logging.error(f"Failed to update podcast: {e}", exc_info=True)
            return error_response("Failed to update podcast.", 500)

    elif req.method == "DELETE":
        try:
            # Optionally, delete all blobs/data for this podcast_id
            # For now, just delete the main metadata blob
            from utils.azure_blob import delete_blob_from_storage
            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: delete_blob_from_storage(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to delete podcast.", 500)
            return json_response({
                "message": "Podcast deleted successfully.",
                "result": {"podcast_id": podcast_id}
            }, 200)
        except Exception as e:
            logging.error(f"Failed to delete podcast: {e}", exc_info=True)
            return error_response("Failed to delete podcast.", 500)

    return error_response("Method Not Allowed", 405)
