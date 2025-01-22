import azure.functions as func
import logging
import json
from utils.azure_blob import load_from_blob_storage, save_to_blob_storage
import pandas as pd


def missing(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Handling request for managing missing episodes.")

    try:
        # Validate HTTP method
        if req.method not in ["GET", "POST"]:
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(
                "Method not allowed. Use GET or POST.", status_code=405
            )

        # Extract instance_id from query parameters or body
        instance_id = req.params.get("instance_id")
        if not instance_id and req.method == "POST":
            try:
                body = req.get_json()
                instance_id = body.get("instance_id")
            except ValueError:
                logging.error("Invalid JSON body.")
                return func.HttpResponse("Invalid JSON body.", status_code=400)

        if not instance_id:
            logging.error("Missing instance_id.")
            return func.HttpResponse("Missing instance_id.", status_code=400)

        # Load blob data
        try:
            blob_data = load_from_blob_storage(instance_id)
            json_data = json.loads(blob_data)
            potential_missing_episodes = json_data.get("data", [])
        except Exception as e:
            logging.error(f"Failed to load blob data: {e}")
            return func.HttpResponse(
                f"Failed to load blob data: {str(e)}", status_code=500
            )

        # Convert to DataFrame for compatibility with utilities
        downloads_df = pd.DataFrame(potential_missing_episodes)

        if req.method == "GET":
            # Return only rows marked as potential missing episodes
            missing_episodes = downloads_df[downloads_df['potential_missing_episode']].to_dict(orient="records")
            return func.HttpResponse(
                json.dumps(missing_episodes),
                mimetype="application/json",
                status_code=200,
            )

        if req.method == "POST":
            # Parse the JSON body
            try:
                body = req.get_json()
                updates = body.get("updates")
                if not updates or not isinstance(updates, list):
                    logging.error("Invalid updates format.")
                    return func.HttpResponse(
                        "Invalid updates format.", status_code=400
                    )
            except ValueError:
                logging.error("Invalid JSON body.")
                return func.HttpResponse("Invalid JSON body.", status_code=400)

            # Process updates
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

                # Update the relevant entry in the DataFrame
                downloads_df.loc[downloads_df['Date'] == date, 'accepted'] = accepted

            # Save updated blob data
            json_data["data"] = downloads_df.to_dict(orient="records")
            try:
                save_to_blob_storage(json.dumps(json_data), instance_id)
            except Exception as e:
                logging.error(f"Failed to save blob data: {e}")
                return func.HttpResponse(
                    f"Failed to save blob data: {str(e)}", status_code=500
                )

            return func.HttpResponse(
                json.dumps({"message": "Updates applied successfully."}),
                mimetype="application/json",
                status_code=200,
            )

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            f"An unexpected error occurred: {str(e)}", status_code=500
        )
