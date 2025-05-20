import azure.functions as func
import logging
import json
from utils.azure_blob import load_from_blob_storage, save_to_blob_storage
from utils.retry import retry_with_backoff
import pandas as pd
from typing import Optional


def missing(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to manage missing episodes (GET: list, POST: update).
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with missing episodes or update status.
    """
    logging.info("Handling request for managing missing episodes.")

    try:
        # Validate HTTP method
        if req.method not in ["GET", "POST"]:
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(
                "Method not allowed. Use GET or POST.", status_code=405
            )

        # Extract instance_id from query parameters or body
        instance_id: Optional[str] = req.params.get("instance_id")
        if not instance_id and req.method == "POST":
            try:
                body = req.get_json()
                instance_id = body.get("instance_id")
            except ValueError:
                logging.error("Invalid JSON body.", exc_info=True)
                return func.HttpResponse("Invalid JSON body.", status_code=400)

        if not instance_id:
            logging.error("Missing instance_id.")
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
            potential_missing_episodes = json_data.get("data", [])
        except Exception as e:
            logging.error(f"Failed to load blob data: {e}", exc_info=True)
            return func.HttpResponse(
                "Failed to load blob data.", status_code=500
            )

        # Convert to DataFrame for compatibility with utilities
        try:
            downloads_df = pd.DataFrame(potential_missing_episodes)
        except Exception as e:
            logging.error(f"Failed to convert blob data to DataFrame: {e}", exc_info=True)
            return func.HttpResponse(
                "Failed to process blob data.", status_code=500
            )

        if req.method == "GET":
            try:
                missing_episodes = downloads_df[downloads_df['potential_missing_episode']].to_dict(orient="records")
                return func.HttpResponse(
                    json.dumps(missing_episodes),
                    mimetype="application/json",
                    status_code=200,
                )
            except Exception as e:
                logging.error(f"Failed to filter missing episodes: {e}", exc_info=True)
                return func.HttpResponse(
                    "Failed to filter missing episodes.", status_code=500
                )

        if req.method == "POST":
            try:
                body = req.get_json()
                updates = body.get("updates")
                if not updates or not isinstance(updates, list):
                    logging.error("Invalid updates format.")
                    return func.HttpResponse(
                        "Invalid updates format.", status_code=400
                    )
            except ValueError:
                logging.error("Invalid JSON body.", exc_info=True)
                return func.HttpResponse("Invalid JSON body.", status_code=400)

            # Process updates
            try:
                for update in updates:
                    date = update.get("date")
                    accepted = update.get("accepted")

                    if not date or not isinstance(accepted, bool):
                        logging.error(
                            f"Invalid update entry: {update}. Must include 'date' and 'accepted'."
                        )
                        return func.HttpResponse(
                            "Invalid update entry. Must include 'date' and 'accepted'.",
                            status_code=400,
                        )

                    downloads_df.loc[downloads_df['Date'] == date, 'accepted'] = accepted
            except Exception as e:
                logging.error(f"Failed to process updates: {e}", exc_info=True)
                return func.HttpResponse(
                    "Failed to process updates.", status_code=500
                )

            # Save updated blob data with retry
            try:
                json_data["data"] = downloads_df.to_dict(orient="records")
                retry_with_backoff(
                    lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )()
            except Exception as e:
                logging.error(f"Failed to save blob data: {e}", exc_info=True)
                return func.HttpResponse(
                    "Failed to save blob data.", status_code=500
                )

            return func.HttpResponse(
                json.dumps({"message": "Updates applied successfully."}),
                mimetype="application/json",
                status_code=200,
            )

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            "An unexpected error occurred.", status_code=500
        )
