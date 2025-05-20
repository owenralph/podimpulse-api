import azure.functions as func
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
from utils.retry import retry_with_backoff
import logging
import json
from typing import Optional

def rss(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get or update the RSS feed URL for an instance.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with the RSS URL or update status.
    """
    logging.info("Received request to handle RSS feed.")

    try:
        # Validate HTTP method
        if req.method == "POST":
            try:
                request_data = req.get_json()
                instance_id: Optional[str] = request_data.get("instance_id")
                rss_url: Optional[str] = request_data.get("rss_url")

                if not instance_id or not rss_url:
                    logging.error("Missing instance_id or rss_url in request body.")
                    return func.HttpResponse("Missing instance_id or rss_url.", status_code=400)

                # Load existing blob data with retry
                try:
                    blob_data = retry_with_backoff(
                        lambda: load_from_blob_storage(instance_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )()
                    json_data = json.loads(blob_data)
                except Exception as e:
                    logging.error(f"Failed to load blob data: {e}", exc_info=True)
                    return func.HttpResponse("Failed to load blob data.", status_code=500)

                # Update or create the rss_url property
                json_data["rss_url"] = rss_url

                # Save updated blob data with retry
                try:
                    retry_with_backoff(
                        lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )()
                except Exception as e:
                    logging.error(f"Failed to save blob data: {e}", exc_info=True)
                    return func.HttpResponse("Failed to save blob data.", status_code=500)

                return func.HttpResponse(
                    json.dumps({"message": "RSS feed URL updated successfully.", "instance_id": instance_id, "rss_url": rss_url}),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception as e:
                logging.error(f"Failed to update RSS feed: {e}", exc_info=True)
                return func.HttpResponse(
                    "Failed to update RSS feed.",
                    status_code=500
                )

        elif req.method == "GET":
            try:
                instance_id: Optional[str] = req.params.get("instance_id")

                if not instance_id:
                    logging.error("Missing instance_id in query parameters.")
                    return func.HttpResponse("Missing instance_id.", status_code=400)

                # Load blob data with retry
                try:
                    blob_data = retry_with_backoff(
                        lambda: load_from_blob_storage(instance_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )()
                    json_data = json.loads(blob_data)
                except Exception as e:
                    logging.error(f"Failed to load blob data: {e}", exc_info=True)
                    return func.HttpResponse("Failed to load blob data.", status_code=500)

                if "rss_url" not in json_data:
                    logging.error("RSS feed URL not set.")
                    return func.HttpResponse("RSS feed URL not set.", status_code=404)

                return func.HttpResponse(
                    json.dumps({"rss_url": json_data["rss_url"]}),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception as e:
                logging.error(f"Failed to retrieve RSS feed: {e}", exc_info=True)
                return func.HttpResponse(
                    "Failed to retrieve RSS feed.",
                    status_code=500
                )

        else:
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse("Method Not Allowed", status_code=405)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            "An unexpected error occurred.",
            status_code=500
        )
