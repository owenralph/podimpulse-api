import azure.functions as func
import logging
import json
from typing import Optional
import pandas as pd
import numpy as np
from utils.azure_blob import load_podcast_blob
from utils.retry import retry_with_backoff
from utils import validate_http_method, handle_blob_operation, error_response, json_response


def trend(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to calculate trend with rolling average and line of best fit.
    """
    logging.debug("[trend] Received request to calculate trend with rolling average and line of best fit.")

    method_error = validate_http_method(req, ["GET"])
    if method_error:
        return method_error

    podcast_id: Optional[str] = req.route_params.get("podcast_id")
    if not podcast_id:
        return error_response("Missing podcast_id in path.", 400)

    days: Optional[str] = req.params.get("days")
    if not days:
        return error_response("Missing 'days' in the request.", 400)

    try:
        days_int = int(days)
        if days_int <= 0:
            raise ValueError("The 'days' parameter must be a positive integer.")
    except ValueError as e:
        return error_response(f"Invalid 'days' parameter: {e}", 400)

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
        return error_response("Error retrieving data from storage.", 404)

    try:
        payload = json.loads(blob_data)
        records = payload.get("data", [])
        if not records:
            return error_response("No ingested data found for this podcast.", 404)
        df = pd.DataFrame(records)
    except Exception as e:
        logging.error(f"Error parsing dataset payload: {e}", exc_info=True)
        return error_response("Error parsing dataset.", 400)

    if "Date" not in df.columns or "Downloads" not in df.columns:
        return error_response("Dataset is missing required columns: Date and Downloads.", 400)

    try:
        df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
        df["Downloads"] = pd.to_numeric(df["Downloads"], errors="coerce")
        df = df.dropna(subset=["Date", "Downloads"]).sort_values("Date")
    except Exception as e:
        logging.error(f"Error normalizing dataset: {e}", exc_info=True)
        return error_response("Error normalizing dataset.", 400)

    if len(df) < days_int:
        return error_response(
            "Not enough data points for the requested rolling window.",
            400
        )

    try:
        df["rolling_average"] = df["Downloads"].rolling(window=days_int).mean()
        result_df = df[["Date", "rolling_average"]].dropna().rename(columns={"Date": "date"})
    except Exception as e:
        logging.error(f"Error calculating rolling average: {e}", exc_info=True)
        return error_response("Error calculating rolling average.", 500)

    if len(result_df) < 2:
        return error_response("Not enough data points after rolling window to fit a trend line.", 400)

    try:
        result_df["date_numeric"] = (result_df["date"] - result_df["date"].min()).dt.days
        slope, intercept = np.polyfit(result_df["date_numeric"], result_df["rolling_average"], 1)
        result_df["line_of_best_fit"] = slope * result_df["date_numeric"] + intercept
        result_df["date"] = result_df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        logging.error(f"Error calculating trend line: {e}", exc_info=True)
        return error_response("Error calculating line of best fit.", 500)

    trend_data = result_df[["date", "rolling_average", "line_of_best_fit"]].to_dict(orient="records")
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
    return json_response(response, 200)
