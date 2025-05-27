import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from utils.azure_blob import blob_container_client
from utils.retry import retry_with_backoff
from io import StringIO
from typing import Optional
from utils import validate_http_method
import pandas as pd
import numpy as np
import json


def trend(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to calculate trend with rolling average and line of best fit.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with trend data or error message.
    """
    logging.debug("[trend] Received request to calculate trend with rolling average and line of best fit.")

    try:
        # Validate HTTP method
        method_error = validate_http_method(req, ["GET", "PUT", "DELETE"])
        if method_error:
            return method_error

        podcast_id: Optional[str] = req.route_params.get("podcast_id")
        if not podcast_id:
            return func.HttpResponse(json.dumps({
                "message": "Missing podcast_id in path.",
                "result": None
            }), status_code=400)

        # For GET: extract days from query
        if req.method == "GET":
            days: Optional[str] = req.params.get('days')

            if not days:
                error_message = "Missing 'days' in the request."
                logging.error(error_message)
                return func.HttpResponse(json.dumps({
                    "message": error_message,
                    "result": None
                }), status_code=400)

            try:
                days_int = int(days)
                if days_int <= 0:
                    raise ValueError("The 'days' parameter must be a positive integer.")
            except ValueError as e:
                error_message = f"Invalid 'days' parameter: {e}"
                logging.error(error_message)
                return func.HttpResponse(json.dumps({
                    "message": error_message,
                    "result": None
                }), status_code=400)

            # Retrieve JSON from Blob Storage with retry
            try:
                def load_blob():
                    return blob_container_client.get_blob_client(podcast_id).download_blob().readall().decode()
                json_data = retry_with_backoff(
                    load_blob,
                    exceptions=(Exception,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )()
            except Exception as e:
                error_message = f"Error retrieving data for podcast_id {podcast_id}: {e}"
                logging.error(error_message, exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error retrieving data from storage.",
                    "result": None
                }), status_code=404)

            # Parse JSON into DataFrame
            try:
                df = pd.read_json(StringIO(json_data), orient="records")
                df['Date'] = pd.to_datetime(df['Date'])
                df.sort_values('Date', inplace=True)
            except Exception as e:
                error_message = f"Error parsing dataset: {e}"
                logging.error(error_message, exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error parsing dataset.",
                    "result": None
                }), status_code=400)

            # Calculate rolling average
            try:
                df['Rolling Average'] = df['Downloads'].rolling(window=days_int).mean()
                result_df = df[['Date', 'Rolling Average']].dropna()
                result_df.rename(columns={
                    'Date': 'date',
                    'Rolling Average': 'rolling_average'
                }, inplace=True)
            except Exception as e:
                error_message = f"Error calculating rolling average: {e}"
                logging.error(error_message, exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error calculating rolling average.",
                    "result": None
                }), status_code=500)

            # Calculate the line of best fit based on the rolling average
            try:
                result_df['date_numeric'] = (result_df['date'] - result_df['date'].min()).dt.days
                slope, intercept = np.polyfit(result_df['date_numeric'], result_df['rolling_average'], 1)
                result_df['line_of_best_fit'] = slope * result_df['date_numeric'] + intercept
                logging.info(f"Line of best fit calculated on rolling average with slope {slope:.4f} and intercept {intercept:.4f}.")
            except Exception as e:
                error_message = f"Error calculating line of best fit: {e}"
                logging.error(error_message, exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error calculating line of best fit.",
                    "result": None
                }), status_code=500)

            # Convert Timestamps to ISO format
            try:
                result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            except Exception as e:
                logging.error(f"Error formatting dates: {e}", exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error formatting dates.",
                    "result": None
                }), status_code=500)

            # Prepare JSON response
            try:
                trend_data = result_df[['date', 'rolling_average', 'line_of_best_fit']].to_dict(orient="records")
                response = {
                    "message": "Trend calculation completed successfully.",
                    "result": {
                        "trend_data": trend_data,
                        "trend_line": {
                            "slope": float(slope),
                            "intercept": float(intercept)
                        }
                    }
                }
                response_json = json.dumps(response)
                logging.info("Trend calculation complete. Returning JSON response.")
                return func.HttpResponse(
                    response_json,
                    mimetype="application/json",
                    status_code=200
                )
            except Exception as e:
                error_message = f"Error preparing JSON response: {e}"
                logging.error(error_message, exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Error preparing JSON response.",
                    "result": None
                }), status_code=500)

        # For PUT/DELETE: implement as needed
        # ...existing code...

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "An unexpected error occurred.",
            "result": None
        }), status_code=500)
