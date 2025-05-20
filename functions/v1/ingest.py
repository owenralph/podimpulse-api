import logging
import azure.functions as func
from utils.csv_parser import parse_csv
from utils.rss_parser import parse_rss_feed
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
from utils.spike_clustering import perform_spike_clustering
from utils.missing_episodes import mark_potential_missing_episodes
from utils.constants import ERROR_METHOD_NOT_ALLOWED, ERROR_MISSING_CSV
from utils.episode_counts import add_episode_counts_and_titles
from utils.retry import retry_with_backoff
import json
import requests
import io
from typing import Optional

def ingest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to ingest podcast data, process CSV and RSS, and update blob storage.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with processed data or error message.
    """
    logging.info("Received request for adding episode release counts, clustering spikes, and detecting missing episodes.")

    try:
        # Validate HTTP method
        if req.method != "POST":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(ERROR_METHOD_NOT_ALLOWED, status_code=405)

        # Parse JSON body
        try:
            request_data = req.get_json()
        except ValueError:
            logging.error("Invalid JSON body", exc_info=True)
            return func.HttpResponse("Invalid JSON body", status_code=400)

        # Validate inputs
        instance_id: Optional[str] = request_data.get('instance_id')
        csv_url: Optional[str] = request_data.get('csv_url')

        if not instance_id:
            logging.error("Missing instance_id in request body.")
            return func.HttpResponse("Missing instance_id.", status_code=400)

        if not csv_url:
            logging.error(ERROR_MISSING_CSV)
            return func.HttpResponse(ERROR_MISSING_CSV, status_code=400)

        # Load blob data to retrieve RSS URL with retry
        try:
            blob_data = retry_with_backoff(
                lambda: load_from_blob_storage(instance_id),
                exceptions=(RuntimeError, ),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            json_data = json.loads(blob_data)
            rss_url = json_data.get("rss_url")
            if not rss_url:
                logging.error("RSS feed URL not set in the blob. Cannot proceed.")
                return func.HttpResponse("RSS feed URL not set. Use POST to create it.", status_code=404)
        except Exception as e:
            logging.error(f"Failed to load blob or retrieve RSS URL: {e}", exc_info=True)
            return func.HttpResponse("Failed to load blob or retrieve RSS URL.", status_code=500)

        # Fetch CSV data from URL with retry
        try:
            def fetch_csv():
                response = requests.get(csv_url, timeout=10)
                response.raise_for_status()
                return response.content.decode('utf-8')
            csv_data = retry_with_backoff(
                fetch_csv,
                exceptions=(requests.RequestException,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
        except Exception as e:
            logging.error(f"Failed to fetch CSV from URL: {e}", exc_info=True)
            return func.HttpResponse("Failed to fetch CSV from URL.", status_code=400)

        # Parse CSV (using StringIO to wrap the string as a file-like object)
        try:
            csv_file_like = io.StringIO(csv_data)
            downloads_df = parse_csv(csv_file_like)
        except Exception as e:
            logging.error(f"Failed to parse CSV: {e}", exc_info=True)
            return func.HttpResponse("Failed to parse CSV file.", status_code=400)

        # Parse RSS feed
        try:
            episode_data = parse_rss_feed(rss_url)
        except Exception as e:
            logging.error(f"Failed to parse RSS feed: {e}", exc_info=True)
            return func.HttpResponse("Failed to parse RSS feed.", status_code=400)

        # Add episode counts and titles to DataFrame
        try:
            downloads_df = add_episode_counts_and_titles(downloads_df, episode_data)
        except Exception as e:
            logging.error(f"Failed to add episode counts/titles: {e}", exc_info=True)
            return func.HttpResponse("Failed to add episode counts/titles.", status_code=500)

        # Perform clustering on spikes
        try:
            downloads_df = perform_spike_clustering(downloads_df, max_clusters=10)
        except Exception as e:
            logging.error(f"Failed to perform spike clustering: {e}", exc_info=True)
            return func.HttpResponse("Failed to perform spike clustering.", status_code=500)

        # Mark potential missing episodes
        try:
            downloads_df = mark_potential_missing_episodes(downloads_df, episode_data["Date"])
        except Exception as e:
            logging.error(f"Failed to mark potential missing episodes: {e}", exc_info=True)
            return func.HttpResponse("Failed to mark potential missing episodes.", status_code=500)

        # Convert to JSON and prepare final blob
        try:
            result_json = downloads_df.to_json(orient="records", date_format="iso")
            json_data["csv_url"] = csv_url
            json_data["data"] = json.loads(result_json)
        except Exception as e:
            logging.error(f"Failed to convert results to JSON: {e}", exc_info=True)
            return func.HttpResponse("Failed to convert results to JSON.", status_code=500)

        # Save updated blob data with retry
        try:
            retry_with_backoff(
                lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                exceptions=(RuntimeError, ),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
        except Exception as e:
            logging.error(f"Failed to save updated blob data: {e}", exc_info=True)
            return func.HttpResponse("Failed to save updated blob data.", status_code=500)

        # Return the data table in a response
        try:
            response = {
                "message": "Data processed successfully.",
                "instance_id": instance_id,
                "data": json_data["data"]
            }
            return func.HttpResponse(
                json.dumps(response),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.error(f"Error preparing response: {e}", exc_info=True)
            return func.HttpResponse("Error preparing response.", status_code=500)

    except ValueError as ve:
        logging.error(str(ve), exc_info=True)
        return func.HttpResponse(str(ve), status_code=400)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse("An unexpected error occurred.", status_code=500)
