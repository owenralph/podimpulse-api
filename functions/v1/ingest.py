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
from utils.seasonality import add_seasonality_predictors
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import json
import requests
import io
import pandas as pd
import numpy as np
from typing import Optional

def ingest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to ingest podcast data, process CSV and RSS, and update blob storage.
    Accepts either a CSV URL (JSON body) or a file upload (multipart/form-data with 'file' field).

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with ingestion results or error message.
    """
    logging.debug("[ingest] Received request for adding episode release counts, clustering spikes, and detecting missing episodes.")
    # Validate HTTP method
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    # Determine content type
    content_type = req.headers.get('Content-Type', '')
    is_multipart = content_type.startswith('multipart/form-data')
    request_data = None
    csv_data = None
    instance_id = None
    csv_url = None

    if is_multipart:
        # Handle file upload
        try:
            # Azure Functions parses files into req.files (if using v2+ SDK), else use req.files() or req.form()
            file = req.files.get('file') if hasattr(req, 'files') else None
            if not file:
                # Try alternate method for older SDKs
                form = req.form if hasattr(req, 'form') else None
                file = form.get('file') if form else None
            if not file:
                logging.error("No file uploaded in multipart/form-data request.")
                return func.HttpResponse(json.dumps({
                    "message": "No file uploaded in multipart/form-data request.",
                    "result": None
                }), status_code=400)
            # Read file content
            csv_data = file.read().decode('utf-8') if hasattr(file, 'read') else file.stream.read().decode('utf-8')
            # Get instance_id from form data
            instance_id = req.form.get('instance_id') if hasattr(req, 'form') else None
            if not instance_id:
                logging.error("Missing instance_id in form data.")
                return func.HttpResponse(json.dumps({
                    "message": "Missing instance_id in form data.",
                    "result": None
                }), status_code=400)
        except Exception as e:
            logging.error(f"Failed to process file upload: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to process file upload.",
                "result": None
            }), status_code=400)
    else:
        # Handle JSON body (CSV URL)
        try:
            request_data = req.get_json()
        except ValueError:
            logging.error("Invalid JSON body", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Invalid JSON body",
                "result": None
            }), status_code=400)
        # Validate inputs
        instance_id = request_data.get('instance_id')
        csv_url = request_data.get('csv_url')
        if not instance_id:
            logging.error("Missing instance_id in request body.")
            return func.HttpResponse(json.dumps({
                "message": "Missing instance_id.",
                "result": None
            }), status_code=400)
        if not csv_url:
            logging.error(ERROR_MISSING_CSV)
            return func.HttpResponse(json.dumps({
                "message": ERROR_MISSING_CSV,
                "result": None
            }), status_code=400)

    # Load blob data to retrieve RSS URL with retry
    blob_data, err = handle_blob_operation(
        retry_with_backoff(
            lambda: load_from_blob_storage(instance_id),
            exceptions=(RuntimeError, ),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0
        )
    )
    if err:
        return error_response("Failed to load blob or retrieve RSS URL.", 500)
    json_data = json.loads(blob_data)
    rss_url = json_data.get("rss_url")
    if not rss_url:
        logging.error("RSS feed URL not set in the blob. Cannot proceed.")
        return error_response("RSS feed URL not set. Use POST to create it.", 404)

    # Fetch or use CSV data
    if csv_data is None:
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
            return func.HttpResponse(json.dumps({
                "message": "Failed to fetch CSV from URL.",
                "result": None
            }), status_code=400)

    # Parse CSV (using StringIO to wrap the string as a file-like object)
    try:
        csv_file_like = io.StringIO(csv_data)
        downloads_df = parse_csv(csv_file_like)
    except Exception as e:
        logging.error(f"Failed to parse CSV: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to parse CSV file.",
            "result": None
        }), status_code=400)

    # Parse RSS feed
    try:
        episode_data = parse_rss_feed(rss_url)
    except Exception as e:
        logging.error(f"Failed to parse RSS feed: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to parse RSS feed.",
            "result": None
        }), status_code=400)

    # Add episode counts and titles to DataFrame
    try:
        downloads_df = add_episode_counts_and_titles(downloads_df, episode_data)
    except Exception as e:
        logging.error(f"Failed to add episode counts/titles: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to add episode counts/titles.",
            "result": None
        }), status_code=500)

    # Perform clustering on spikes
    try:
        downloads_df = perform_spike_clustering(downloads_df, max_clusters=10)
    except Exception as e:
        logging.error(f"Failed to perform spike clustering: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to perform spike clustering.",
            "result": None
        }), status_code=500)

    # Mark potential missing episodes
    try:
        downloads_df, missing_episodes = mark_potential_missing_episodes(downloads_df, episode_data["Date"], return_missing=True)
    except Exception as e:
        logging.error(f"Failed to mark potential missing episodes: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to mark potential missing episodes.",
            "result": None
        }), status_code=500)

    # Add seasonality predictors to the DataFrame
    try:
        downloads_df = add_seasonality_predictors(downloads_df, date_col='Date')
    except Exception as e:
        logging.error(f"Failed to add seasonality predictors: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to add seasonality predictors.",
            "result": None
        }), status_code=500)

    # Convert to JSON and prepare final blob
    try:
        # Convert 'Date' column to UK local time and add timezone indicator
        local_dt = downloads_df['Date'].dt.tz_convert('Europe/London')
        downloads_df['Date'] = local_dt.dt.strftime('%Y-%m-%dT%H:%M:%S')
        # Add a new column for timezone indicator (BST/GMT)
        downloads_df['timezone'] = local_dt.dt.strftime('%Z')
        result_json = downloads_df.to_json(orient="records")
        if csv_url:
            json_data["csv_url"] = csv_url
        json_data["data"] = json.loads(result_json)
    except Exception as e:
        logging.error(f"Failed to convert results to JSON: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to convert results to JSON.",
            "result": None
        }), status_code=500)

    # Save updated blob data with retry
    _, err = handle_blob_operation(
        retry_with_backoff(
            lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
            exceptions=(RuntimeError, ),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0
        )
    )
    if err:
        return error_response("Failed to save updated blob data.", 500)

    # Return the data table in a response
    try:
        # Ensure potential_missing_episodes dates match the output format and timezone
        if 'Date' in downloads_df.columns:
            downloads_df['Date'] = pd.to_datetime(downloads_df['Date'])
            if downloads_df['Date'].dt.tz is None or str(downloads_df['Date'].dt.tz) == 'None':
                downloads_df['Date'] = downloads_df['Date'].dt.tz_localize('UTC').dt.tz_convert('Europe/London')
            else:
                downloads_df['Date'] = downloads_df['Date'].dt.tz_convert('Europe/London')
            downloads_df['Date'] = downloads_df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        missing_dates = downloads_df.loc[downloads_df['potential_missing_episode'], 'Date']
        missing_dates_list = list(missing_dates)
        response = {
            "message": "Data processed successfully.",
            "result": {
                "instance_id": instance_id,
                "data": json_data["data"],
                "potential_missing_episodes": missing_dates_list
            }
        }
        return json_response(response, 200)
    except Exception as e:
        logging.error(f"Error preparing response: {e}", exc_info=True)
        return error_response("Error preparing response.", 500)

    except ValueError as ve:
        logging.error(str(ve), exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": str(ve),
            "result": None
        }), status_code=400)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "An unexpected error occurred.",
            "result": None
        }), status_code=500)
