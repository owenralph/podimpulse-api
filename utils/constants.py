from dotenv import load_dotenv
import os
import pandas as pd
import pytz

load_dotenv()

# General Constants
TIMEZONE = 'Europe/London'

# Error Messages
ERROR_MISSING_CSV = "Missing 'csv_file' in the request. Please upload a valid CSV file."
ERROR_MISSING_RSS = "Missing 'rss_url' in the request. Please provide a valid RSS feed URL."
ERROR_METHOD_NOT_ALLOWED = "Invalid HTTP method. Only POST requests are allowed."

# Azure Blob Storage
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "podcast-data"

# Facebook API
APP_ID = os.getenv("FACEBOOK_APP_ID")
APP_SECRET = os.getenv("FACEBOOK_APP_SECRET")

def normalize_to_london_date(date_series):
    """
    Normalize a pandas Series of dates to Europe/London local time (with DST),
    returning a tz-aware, normalized date series.
    """
    london_tz = pytz.timezone('Europe/London')
    # Always parse as UTC first, then convert
    return pd.to_datetime(date_series, utc=True, errors='coerce').dt.tz_convert(london_tz).dt.normalize()

def get_default_rss_day_properties():
    """
    Returns a dict of default RSS day properties for a new date.
    """
    return {
        'episode_titles': [],
        'episodes_released': 0,
        'clustered_episode_titles': [],
        'timezone': 'BST',  # Default, or use a constant if available
    }
