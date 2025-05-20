import azure.functions as func
from utils.azure_blob import save_to_blob_storage
from utils.retry import retry_with_backoff
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
        func.HttpResponse: The HTTP response with the new instance_id or error message.
    """
    start_time = time.time()
    logging.info("Received request to initialize a new instance.")

    try:
        # Validate HTTP method
        if req.method != "POST":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse("Method Not Allowed", status_code=405)

        # Create an empty JSON object
        empty_json = "{}"

        # Save the empty JSON to Azure Blob Storage with retry
        try:
            save_start = time.time()
            instance_id = retry_with_backoff(
                lambda: save_to_blob_storage(empty_json),
                exceptions=(RuntimeError,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            save_duration = time.time() - save_start
            logging.info(f"Blob save completed in {save_duration:.2f} seconds.")
        except Exception as e:
            logging.error(f"Failed to save to Azure Blob Storage: {e}", exc_info=True)
            return func.HttpResponse(
                "Failed to initialize instance.", 
                status_code=500
            )

        # Return the instance ID in a JSON response
        try:
            response_data = {"instance_id": instance_id}
            total_duration = time.time() - start_time
            logging.info(f"Total function execution time: {total_duration:.2f} seconds.")
            return func.HttpResponse(
                json.dumps(response_data),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.error(f"Failed to prepare response: {e}", exc_info=True)
            return func.HttpResponse(
                "Failed to prepare response.",
                status_code=500
            )

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            "An unexpected error occurred.", 
            status_code=500
        )
