import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from utils.azure_blob import blob_container_client
from io import StringIO
import pandas as pd
import numpy as np
import json


def trend(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received request to calculate trend with rolling average and line of best fit.")

    try:
        # Validate HTTP method
        if req.method != "GET":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(
                "Invalid HTTP method. Only GET requests are allowed.",
                status_code=405
            )

        # Extract and validate inputs
        token = req.params.get('token')
        days = req.params.get('days')

        if not token:
            error_message = "Missing 'token' in the request."
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)

        if not days:
            error_message = "Missing 'days' in the request."
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)

        try:
            days = int(days)
            if days <= 0:
                raise ValueError("The 'days' parameter must be a positive integer.")
        except ValueError as e:
            error_message = f"Invalid 'days' parameter: {e}"
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)

        # Retrieve JSON from Blob Storage
        try:
            blob_name = f"{token}.json"
            blob_client = blob_container_client.get_blob_client(blob_name)
            json_data = blob_client.download_blob().readall().decode('utf-8')
        except Exception as e:
            error_message = f"Error retrieving data for token {token}: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=404)

        # Parse JSON into DataFrame
        try:
            df = pd.read_json(StringIO(json_data), orient="records")
            df['Date'] = pd.to_datetime(df['Date'])
            df.sort_values('Date', inplace=True)
        except Exception as e:
            error_message = f"Error parsing dataset: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=400)

        # Calculate rolling average
        try:
            df['Rolling Average'] = df['Downloads'].rolling(window=days).mean()
            result_df = df[['Date', 'Rolling Average']].dropna()

            # Rename columns for JSON output
            result_df.rename(columns={
                'Date': 'date',
                'Rolling Average': 'rolling_average'
            }, inplace=True)
        except Exception as e:
            error_message = f"Error calculating rolling average: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=500)

        # Calculate the line of best fit based on the rolling average
        try:
            result_df['date_numeric'] = (result_df['date'] - result_df['date'].min()).dt.days
            slope, intercept = np.polyfit(result_df['date_numeric'], result_df['rolling_average'], 1)
            result_df['line_of_best_fit'] = slope * result_df['date_numeric'] + intercept
            logging.info(f"Line of best fit calculated on rolling average with slope {slope:.4f} and intercept {intercept:.4f}.")
        except Exception as e:
            error_message = f"Error calculating line of best fit: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=500)

        # Convert Timestamps to ISO format
        result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Prepare JSON response
        try:
            trend_data = result_df[['date', 'rolling_average', 'line_of_best_fit']].to_dict(orient="records")
            response = {
                "trend_data": trend_data,
                "trend_line": {
                    "slope": slope,
                    "intercept": intercept
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
            return func.HttpResponse(error_message, status_code=500)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            f"An unexpected error occurred: {str(e)}",
            status_code=500
        )
