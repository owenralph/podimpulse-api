from dotenv import load_dotenv
import os

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
