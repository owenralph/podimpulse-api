import azure.functions as func
from utils.azure_blob import save_to_blob_storage
import logging
import json
import time

def initialize(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()
    logging.info("Received request to initialize a new instance.")

    try:
        # Validate HTTP method
        if req.method != "POST":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse("Method Not Allowed", status_code=405)

        # Create an empty JSON object
        empty_json = "{}"

        # Save the empty JSON to Azure Blob Storage
        try:
            save_start = time.time()
            instance_id = save_to_blob_storage(empty_json)
            save_duration = time.time() - save_start
            logging.info(f"Blob save completed in {save_duration:.2f} seconds.")
        except Exception as e:
            logging.error(f"Failed to save to Azure Blob Storage: {e}")
            return func.HttpResponse(
                "Failed to initialize instance.", 
                status_code=500
            )

        # Return the instance ID in a JSON response
        response_data = {"instance_id": instance_id}
        total_duration = time.time() - start_time
        logging.info(f"Total function execution time: {total_duration:.2f} seconds.")
        return func.HttpResponse(
            json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            f"An unexpected error occurred: {str(e)}", 
            status_code=500
        )
