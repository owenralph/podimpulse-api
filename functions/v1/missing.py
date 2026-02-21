import azure.functions as func
import logging
import json
from utils.azure_blob import load_podcast_blob, save_podcast_blob
from utils.retry import retry_with_backoff
import pandas as pd
from typing import Optional
from utils import validate_http_method, json_response, handle_blob_operation, error_response


def missing(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to manage missing episodes (GET: list, POST: update).
    Complies with OpenAPI spec: podcast_id is always from the path, not query/body.
    """
    logging.debug("[missing] Handling request for managing missing episodes.")
    # Validate HTTP method
    method_error = validate_http_method(req, ["GET", "POST"])
    if method_error:
        return method_error
    # Extract podcast_id from route parameters (per OpenAPI spec)
    podcast_id: Optional[str] = req.route_params.get("podcast_id")
    if not podcast_id:
        logging.error("Missing podcast_id in path.")
        return error_response("Missing podcast_id in path.", 400)
    # Load blob data with retry
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
    potential_missing_episodes = json_data.get("data", [])
    # Convert to DataFrame for compatibility with utilities
    try:
        downloads_df = pd.DataFrame(potential_missing_episodes)
    except Exception as e:
        logging.error(f"Failed to convert blob data to DataFrame: {e}", exc_info=True)
        return error_response("Failed to process blob data.", 500)
    # Defensive: If 'potential_missing_episode' is missing, add it as all False
    if 'potential_missing_episode' not in downloads_df.columns:
        downloads_df['potential_missing_episode'] = False
    if req.method == "GET":
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
            return json_response({
                "message": "Missing episodes retrieved successfully.",
                "result": {"potential_missing_episodes": missing_dates_list}
            }, 200)
        except Exception as e:
            logging.error(f"Failed to filter missing episodes: {e}", exc_info=True)
            return error_response("Failed to filter missing episodes.", 500)
    if req.method == "POST":
        try:
            body = req.get_json()
            # Per OpenAPI spec, podcast_id and updates are in the body
            # But we already have podcast_id from the path, so only use updates
            updates = body.get("updates")
            if updates == 'ALL':
                updates = [
                    {'date': d, 'accepted': True}
                    for d in pd.to_datetime(downloads_df.loc[downloads_df['potential_missing_episode'], 'Date']).dt.strftime('%Y-%m-%d').tolist()
                ]
            if not updates or not isinstance(updates, list):
                logging.error("Invalid updates format.")
                return error_response("Invalid updates format.", 400)
        except ValueError:
            logging.error("Invalid JSON body.", exc_info=True)
            return error_response("Invalid JSON body.", 400)
        # Process updates
        try:
            downloads_df['Date'] = pd.to_datetime(downloads_df['Date'])
            for update in updates:
                date = update.get("date")
                accepted = update.get("accepted")
                if not date or not isinstance(accepted, bool):
                    logging.error(f"Invalid update entry: {update}. Must include 'date' and 'accepted'.")
                    return error_response("Invalid update entry. Must include 'date' and 'accepted'.", 400)
                mask = downloads_df['Date'].dt.strftime('%Y-%m-%d') == date
                downloads_df.loc[mask, 'accepted'] = accepted
                if accepted:
                    if 'Episodes Released' in downloads_df.columns:
                        downloads_df.loc[mask, 'Episodes Released'] = (
                            downloads_df.loc[mask, 'Episodes Released'].fillna(0).astype(int) + 1
                        )
                    else:
                        downloads_df.loc[mask, 'Episodes Released'] = 1
                    downloads_df.loc[mask, 'potential_missing_episode'] = False
        except Exception as e:
            logging.error(f"Failed to process updates: {e}", exc_info=True)
            return error_response("Failed to process updates.", 500)
        # Save updated blob data with retry
        if 'Date' in downloads_df.columns:
            downloads_df['Date'] = downloads_df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        json_data["data"] = downloads_df.to_dict(orient="records")
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
            return error_response("Failed to save blob data.", 500)
        # Return the full updated dataframe in the same format as ingest
        try:
            missing_dates = downloads_df.loc[downloads_df['potential_missing_episode'], 'Date']
            missing_dates_list = list(missing_dates)
            response = {
                "message": "Updates applied successfully.",
                "result": {
                    "podcast_id": podcast_id,
                    "data": downloads_df.to_dict(orient="records"),
                    "potential_missing_episodes": missing_dates_list
                }
            }
            return json_response(response, 200)
        except Exception as e:
            logging.error(f"Error preparing response: {e}", exc_info=True)
            return error_response("Error preparing response.", 500)
    return error_response("Method Not Allowed", 405)
