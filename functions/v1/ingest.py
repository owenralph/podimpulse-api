import logging
import azure.functions as func
from utils.csv_parser import parse_csv, validate_downloads_dataframe
from utils.rss_parser import parse_rss_feed
from utils.azure_blob import load_podcast_blob, save_podcast_blob
from utils.spike_clustering import perform_spike_clustering
from utils.missing_episodes import mark_potential_missing_episodes
from utils.constants import ERROR_MISSING_CSV
from utils.episode_counts import add_episode_counts_and_titles
from utils.retry import retry_with_backoff
from utils.seasonality import add_seasonality_predictors
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import json
import requests
import io
import pandas as pd
import time
from datetime import datetime, timezone
from typing import Optional

RSS_CACHE_KEY = "_rss_episode_cache"
RSS_CACHE_TTL_SECONDS = 6 * 60 * 60
RSS_CACHE_MAX_EPISODES = 5000


def _episode_df_from_cache(cache_payload, allow_stale: bool = False) -> Optional[pd.DataFrame]:
    if not isinstance(cache_payload, dict):
        return None

    fetched_at = cache_payload.get("fetched_at")
    episodes = cache_payload.get("episodes")
    if not isinstance(episodes, list) or not episodes:
        return None

    try:
        fetched_at_ts = pd.to_datetime(fetched_at, utc=True, errors="coerce")
    except Exception:
        fetched_at_ts = pd.NaT

    if pd.isna(fetched_at_ts):
        return None

    age_seconds = (datetime.now(timezone.utc) - fetched_at_ts.to_pydatetime()).total_seconds()
    if not allow_stale and age_seconds > RSS_CACHE_TTL_SECONDS:
        return None

    df = pd.DataFrame(episodes)
    if "Date" not in df.columns or "Title" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Date"])
    if df.empty:
        return None

    return df[["Date", "Title"]]


def _update_episode_cache(json_data: dict, episode_data: pd.DataFrame) -> None:
    if episode_data is None or episode_data.empty:
        return

    cache_df = episode_data.copy()
    cache_df["Date"] = pd.to_datetime(cache_df["Date"], utc=True, errors="coerce")
    cache_df = cache_df.dropna(subset=["Date"])
    if cache_df.empty:
        return

    cache_df["Title"] = cache_df["Title"].fillna("").astype(str)
    cache_df = cache_df[cache_df["Title"].str.strip() != ""]
    if cache_df.empty:
        return

    cache_df = cache_df.drop_duplicates(subset=["Date", "Title"])
    cache_df = cache_df.sort_values("Date").tail(RSS_CACHE_MAX_EPISODES)
    records = [
        {"Date": row["Date"].isoformat(), "Title": row["Title"]}
        for _, row in cache_df.iterrows()
    ]
    if records:
        json_data[RSS_CACHE_KEY] = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "episodes": records,
        }


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
    method_error = validate_http_method(req, ["POST", "GET", "DELETE"])
    if method_error:
        return method_error

    podcast_id: Optional[str] = req.route_params.get("podcast_id")
    if not podcast_id:
        return error_response("Missing podcast_id in path.", 400)

    # GET: retrieve ingested data
    if req.method == "GET":
        try:
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_podcast_blob(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load ingested data.", 500)
            json_data = json.loads(blob_data)
            return json_response({
                "message": "Podcast data retrieved successfully.",
                "result": json_data.get("data", [])
            }, 200)
        except Exception as e:
            logging.error(f"Failed to retrieve ingested data: {e}", exc_info=True)
            return error_response("Failed to retrieve ingested data.", 500)

    # DELETE: clear ingested data
    if req.method == "DELETE":
        try:
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_podcast_blob(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load blob data.", 500)
            json_data = json.loads(blob_data)
            json_data["data"] = []
            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: save_podcast_blob(json.dumps(json_data), podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to clear ingested data.", 500)
            return func.HttpResponse(status_code=204)
        except Exception as e:
            logging.error(f"Failed to clear ingested data: {e}", exc_info=True)
            return error_response("Failed to clear ingested data.", 500)

    # Determine content type
    content_type = req.headers.get('Content-Type', '')
    is_multipart = content_type.startswith('multipart/form-data')
    request_data = None
    csv_data = None
    csv_url = None
    query_params = getattr(req, "params", {}) or {}
    frequency_mode = query_params.get("frequency_mode", "strict")

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
        csv_url = request_data.get('csv_url')
        frequency_mode = request_data.get("frequency_mode", frequency_mode)
        if not csv_url:
            logging.error(ERROR_MISSING_CSV)
            return func.HttpResponse(json.dumps({
                "message": ERROR_MISSING_CSV,
                "result": None
            }), status_code=400)

    allowed_frequency_modes = {"strict", "resample_daily"}
    if frequency_mode not in allowed_frequency_modes:
        return func.HttpResponse(json.dumps({
            "message": (
                f"Invalid frequency_mode '{frequency_mode}'. "
                f"Allowed values: {sorted(allowed_frequency_modes)}."
            ),
            "result": None
        }), status_code=400)

    # Load blob data to retrieve RSS URL with retry
    blob_data, err = handle_blob_operation(
        retry_with_backoff(
            lambda: load_podcast_blob(podcast_id),
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
                call_start = time.perf_counter()
                response = requests.get(csv_url, timeout=10)
                elapsed_ms = (time.perf_counter() - call_start) * 1000
                logging.info(
                    f"[metric] external_http.call operation=ingest.csv_fetch status={response.status_code} "
                    f"duration_ms={elapsed_ms:.2f} timeout_s=10"
                )
                response.raise_for_status()
                return response.content.decode('utf-8')
            csv_data = retry_with_backoff(
                fetch_csv,
                exceptions=(requests.RequestException,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0,
                operation_name="ingest.csv_fetch"
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
        downloads_df = validate_downloads_dataframe(downloads_df, frequency_mode=frequency_mode)
        ingestion_warnings = []
        frequency_warning = downloads_df.attrs.get("input_frequency_warning")
        if frequency_warning:
            ingestion_warnings.append(frequency_warning)
    except ValueError as e:
        logging.warning(f"CSV validation failed: {e}")
        return func.HttpResponse(json.dumps({
            "message": str(e),
            "result": None
        }), status_code=400)
    except Exception as e:
        logging.error(f"Failed to parse CSV: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "Failed to parse CSV file.",
            "result": None
        }), status_code=400)

    # Parse RSS feed (use fresh cache when possible, fall back to stale cache on fetch failure)
    episode_data = _episode_df_from_cache(json_data.get(RSS_CACHE_KEY))
    if episode_data is not None:
        logging.info("[metric] ingest.rss source=cache freshness=fresh")
    else:
        try:
            rss_start = time.perf_counter()
            episode_data = parse_rss_feed(rss_url)
            rss_elapsed_ms = (time.perf_counter() - rss_start) * 1000
            logging.info(
                "[metric] ingest.rss source=network duration_ms=%.2f",
                rss_elapsed_ms,
            )
            _update_episode_cache(json_data, episode_data)
        except Exception as e:
            stale_episode_data = _episode_df_from_cache(
                json_data.get(RSS_CACHE_KEY),
                allow_stale=True,
            )
            if stale_episode_data is not None:
                logging.warning(f"RSS refresh failed; using stale cache: {e}")
                episode_data = stale_episode_data
                ingestion_warnings.append("RSS feed refresh failed; using cached episode metadata.")
            else:
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
        downloads_df, _missing_episodes = mark_potential_missing_episodes(downloads_df, episode_data["Date"], return_missing=True)
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
        if ingestion_warnings:
            json_data["ingest_warnings"] = ingestion_warnings
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
            lambda: save_podcast_blob(json.dumps(json_data), podcast_id),
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
                "podcast_id": podcast_id,
                "data": json_data["data"],
                "potential_missing_episodes": missing_dates_list
            }
        }
        if ingestion_warnings:
            response["result"]["warnings"] = ingestion_warnings
        return json_response(response, 200)
    except Exception as e:
        logging.error(f"Error preparing response: {e}", exc_info=True)
        return error_response("Error preparing response.", 500)
