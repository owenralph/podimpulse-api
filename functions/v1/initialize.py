import azure.functions as func
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import logging
import json
import time
from typing import Optional

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
    from utils.azure_blob import save_to_blob_storage
    from utils.retry import retry_with_backoff
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
