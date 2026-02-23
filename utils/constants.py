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
_AZURE_WEBJOBS_STORAGE = os.getenv("AzureWebJobsStorage")
_LOCAL_AZURITE_BLOB_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
if not BLOB_CONNECTION_STRING and _AZURE_WEBJOBS_STORAGE:
    if _AZURE_WEBJOBS_STORAGE == "UseDevelopmentStorage=true":
        BLOB_CONNECTION_STRING = _LOCAL_AZURITE_BLOB_CONNECTION_STRING
    else:
        BLOB_CONNECTION_STRING = _AZURE_WEBJOBS_STORAGE
BLOB_CONTAINER_NAME = "podcast-data"

# Facebook API
APP_ID = os.getenv("FACEBOOK_APP_ID")
APP_SECRET = os.getenv("FACEBOOK_APP_SECRET")

# TikTok API
TIKTOK_CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY")
TIKTOK_CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")
