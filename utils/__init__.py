import logging
import json
import azure.functions as func
from typing import List
import pandas as pd

def validate_http_method(req, allowed_methods):
    if req.method not in allowed_methods:
        logging.error(f"Invalid HTTP method: {req.method}")
        return func.HttpResponse(
            json.dumps({"message": "Method Not Allowed", "result": None}),
            status_code=405
        )
    return None

def json_response(data, status_code=200):
    return func.HttpResponse(
        json.dumps(data),
        mimetype="application/json",
        status_code=status_code
    )

def handle_blob_operation(blob_func, *args, **kwargs):
    try:
        return blob_func(*args, **kwargs), None
    except Exception as e:
        logging.error(f"Blob operation failed: {e}", exc_info=True)
        return None, str(e)

def error_response(message, status_code=500):
    return func.HttpResponse(
        json.dumps({"message": message, "result": None}),
        status_code=status_code
    )

def handle_errors(func):
    """
    Decorator to standardize error logging and exception handling for utility functions.
    Logs the error and re-raises the exception.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper

def require_columns(df: pd.DataFrame, columns: List[str]) -> None:
    """
    Checks that all required columns are present in the DataFrame.
    Raises ValueError if any are missing.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
