import azure.functions as func
from utils import validate_http_method, json_response, handle_blob_operation, error_response
from utils.retry import retry_with_backoff
import logging
import json
from typing import Optional
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage

def rss(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get or update the RSS feed URL for an instance.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the RSS feed URL or error message.
    """
    logging.debug("[rss] Received request to handle RSS feed.")
    # Validate HTTP method
    method_error = validate_http_method(req, ["GET", "POST"])
    if method_error:
        return method_error

    if req.method == "POST":
        try:
            request_data = req.get_json()
            instance_id: Optional[str] = request_data.get("instance_id")
            rss_url: Optional[str] = request_data.get("rss_url")

            if not instance_id or not rss_url:
                return error_response("Missing instance_id or rss_url.", 400)

            # Load existing blob data with retry
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(instance_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load blob data.", 500)
            json_data = json.loads(blob_data)

            # Update or create the rss_url property
            json_data["rss_url"] = rss_url

            # Save updated blob data with retry
            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to save blob data.", 500)

            return json_response({
                "message": "RSS feed URL updated successfully.",
                "result": {"instance_id": instance_id, "rss_url": rss_url}
            }, 200)

        except Exception as e:
            logging.error(f"Failed to update RSS feed: {e}", exc_info=True)
            return error_response("Failed to update RSS feed.", 500)

    elif req.method == "GET":
        try:
            instance_id: Optional[str] = req.params.get("instance_id")

            if not instance_id:
                return error_response("Missing instance_id.", 400)
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(instance_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load blob data.", 500)
            json_data = json.loads(blob_data)

            if "rss_url" not in json_data:
                return error_response("RSS feed URL not set.", 404)

            return json_response({
                "message": "RSS feed URL retrieved successfully.",
                "result": {"rss_url": json_data["rss_url"]}
            }, 200)

        except Exception as e:
            logging.error(f"Failed to retrieve RSS feed: {e}", exc_info=True)
            return error_response("Failed to retrieve RSS feed.", 500)

    else:
        logging.error(f"Invalid HTTP method: {req.method}")
        return func.HttpResponse(json.dumps({
            "message": "Method Not Allowed",
            "result": None
        }), status_code=405)

    return func.HttpResponse(
        json.dumps({
            "message": "An unexpected error occurred.",
            "result": None
        }),
        status_code=500
    )
