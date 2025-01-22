import azure.functions as func
from utils.csv_parser import parse_csv
from utils.rss_parser import parse_rss_feed
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
from utils.spike_clustering import perform_spike_clustering
from utils.missing_episodes import mark_potential_missing_episodes
import logging
from utils.constants import ERROR_METHOD_NOT_ALLOWED, ERROR_MISSING_CSV
from utils.episode_counts import add_episode_counts_and_titles
import json
import requests
import io

def ingest(req: func.HttpRequest) -> func.HttpResponse:
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
            logging.error("Invalid JSON body")
            return func.HttpResponse("Invalid JSON body", status_code=400)

        # Validate inputs
        instance_id = request_data.get('instance_id')
        csv_url = request_data.get('csv_url')

        if not instance_id:
            logging.error("Missing instance_id in request body.")
            return func.HttpResponse("Missing instance_id.", status_code=400)

        if not csv_url:
            logging.error(ERROR_MISSING_CSV)
            return func.HttpResponse(ERROR_MISSING_CSV, status_code=400)

        # Load blob data to retrieve RSS URL
        try:
            blob_data = load_from_blob_storage(instance_id)
            json_data = json.loads(blob_data)
            rss_url = json_data.get("rss_url")

            if not rss_url:
                logging.error("RSS feed URL not set in the blob. Cannot proceed.")
                return func.HttpResponse("RSS feed URL not set. Use POST to create it.", status_code=404)
        except Exception as e:
            logging.error(f"Failed to load blob or retrieve RSS URL: {e}")
            return func.HttpResponse(f"Failed to load blob or retrieve RSS URL: {str(e)}", status_code=500)

        # Fetch CSV data from URL
        try:
            response = requests.get(csv_url)
            response.raise_for_status()  # Raise an error for HTTP issues
            csv_data = response.content.decode('utf-8')
        except requests.RequestException as e:
            logging.error(f"Failed to fetch CSV from URL: {e}")
            return func.HttpResponse(f"Failed to fetch CSV from URL: {str(e)}", status_code=400)

        # Parse CSV (using StringIO to wrap the string as a file-like object)
        csv_file_like = io.StringIO(csv_data)
        downloads_df = parse_csv(csv_file_like)

        # Parse RSS feed
        episode_data = parse_rss_feed(rss_url)

        # Add episode counts and titles to DataFrame
        downloads_df = add_episode_counts_and_titles(downloads_df, episode_data)

        # Perform clustering on spikes
        downloads_df = perform_spike_clustering(downloads_df, max_clusters=10)

        # Mark potential missing episodes
        downloads_df = mark_potential_missing_episodes(downloads_df, episode_data["Date"])

        # Convert to JSON and prepare final blob
        result_json = downloads_df.to_json(orient="records", date_format="iso")
        json_data["csv_url"] = csv_url
        json_data["data"] = json.loads(result_json)

        # Save updated blob data
        save_to_blob_storage(json.dumps(json_data), instance_id)

        # Return the data table in a response
        return func.HttpResponse(
            json.dumps({"message": "Data processed successfully.", "instance_id": instance_id, "data": json_data["data"]}),
            mimetype="application/json",
            status_code=200
        )

    except ValueError as ve:
        logging.error(str(ve), exc_info=True)
        return func.HttpResponse(str(ve), status_code=400)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(f"An unexpected error occurred: {str(e)}", status_code=500)
