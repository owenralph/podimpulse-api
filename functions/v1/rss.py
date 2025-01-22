import azure.functions as func
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
import logging
import json

def rss(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received request to handle RSS feed.")

    try:
        # Validate HTTP method
        if req.method == "POST":
            try:
                # Parse request body
                request_data = req.get_json()
                instance_id = request_data.get("instance_id")
                rss_url = request_data.get("rss_url")

                if not instance_id or not rss_url:
                    logging.error("Missing instance_id or rss_url in request body.")
                    return func.HttpResponse("Missing instance_id or rss_url.", status_code=400)

                # Load existing blob data
                blob_data = load_from_blob_storage(instance_id)
                json_data = json.loads(blob_data)

                # Update or create the rss_url property
                json_data["rss_url"] = rss_url

                # Save updated blob data
                save_to_blob_storage(json.dumps(json_data), instance_id)

                return func.HttpResponse(
                    json.dumps({"message": "RSS feed URL updated successfully.", "instance_id": instance_id, "rss_url": rss_url}),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception as e:
                logging.error(f"Failed to update RSS feed: {e}")
                return func.HttpResponse(
                    f"Failed to update RSS feed: {str(e)}", 
                    status_code=500
                )

        elif req.method == "GET":
            try:
                # Extract instance_id from query parameters
                instance_id = req.params.get("instance_id")

                if not instance_id:
                    logging.error("Missing instance_id in query parameters.")
                    return func.HttpResponse("Missing instance_id.", status_code=400)

                # Load blob data
                blob_data = load_from_blob_storage(instance_id)
                json_data = json.loads(blob_data)

                # Check if rss_url exists
                if "rss_url" not in json_data:
                    logging.error("RSS feed URL not set.")
                    return func.HttpResponse("RSS feed URL not set.", status_code=404)

                # Return the rss_url
                return func.HttpResponse(
                    json.dumps({"rss_url": json_data["rss_url"]}),
                    mimetype="application/json",
                    status_code=200
                )

            except Exception as e:
                logging.error(f"Failed to retrieve RSS feed: {e}")
                return func.HttpResponse(
                    f"Failed to retrieve RSS feed: {str(e)}", 
                    status_code=500
                )

        else:
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse("Method Not Allowed", status_code=405)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            f"An unexpected error occurred: {str(e)}", 
            status_code=500
        )
