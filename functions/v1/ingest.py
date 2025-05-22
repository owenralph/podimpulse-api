import logging
import json
import time
import difflib
import io
import requests
import pandas as pd
import numpy as np
import azure.functions as func
from utils.csv_parser import parse_csv
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
from utils.spike_clustering import perform_spike_clustering
from utils.missing_episodes import mark_potential_missing_episodes
from utils.constants import ERROR_METHOD_NOT_ALLOWED, ERROR_MISSING_CSV, normalize_to_london_date, get_default_rss_day_properties
from utils.episode_counts import add_episode_counts_and_titles
from utils.retry import retry_with_backoff
from utils.seasonality import add_time_series_features
from typing import Optional

def ingest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to ingest podcast data, process CSV and update blob storage.
    Accepts either a CSV URL (JSON body) or a file upload (multipart/form-data with 'file' field).
    """
    logging.info("Received request for adding episode release counts, clustering spikes, and detecting missing episodes.")

    try:
        # Validate HTTP method
        if req.method not in ("POST", "PUT"):
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(json.dumps({
                "message": ERROR_METHOD_NOT_ALLOWED,
                "result": None
            }), status_code=405)

        # Determine content type
        content_type = req.headers.get('Content-Type', '')
        is_multipart = content_type.startswith('multipart/form-data')
        request_data = None
        csv_data = None
        instance_id = None
        csv_url = None

        if is_multipart:
            # Handle file upload
            try:
                # Azure Functions parses files into req.files (if using v2+ SDK), else use req.files() or req.form()
                file = req.files.get('file') if hasattr(req, 'files') else None
                if not file:
                    # Try alternate method for older SDKs
                    form = req.form if hasattr(req, 'form') else None
                    file = form.get('file') if form else None
                if not file:
                    logging.error("No file uploaded in multipart/form-data request.")
                    return func.HttpResponse(json.dumps({
                        "message": "No file uploaded in multipart/form-data request.",
                        "result": None
                    }), status_code=400)
                # Read file content
                csv_data = file.read().decode('utf-8') if hasattr(file, 'read') else file.stream.read().decode('utf-8')
                # Get instance_id from form data
                instance_id = req.form.get('instance_id') if hasattr(req, 'form') else None
                if not instance_id:
                    logging.error("Missing instance_id in form data.")
                    return func.HttpResponse(json.dumps({
                        "message": "Missing instance_id in form data.",
                        "result": None
                    }), status_code=400)
            except Exception as e:
                logging.error(f"Failed to process file upload: {e}", exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Failed to process file upload.",
                    "result": None
                }), status_code=400)
        else:
            # Handle JSON body (CSV URL)
            try:
                request_data = req.get_json()
            except ValueError:
                logging.error("Invalid JSON body", exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Invalid JSON body",
                    "result": None
                }), status_code=400)
            # Validate inputs
            instance_id = request_data.get('instance_id')
            csv_url = request_data.get('csv_url')
            if not instance_id:
                logging.error("Missing instance_id in request body.")
                return func.HttpResponse(json.dumps({
                    "message": "Missing instance_id.",
                    "result": None
                }), status_code=400)
            if not csv_url:
                logging.error(ERROR_MISSING_CSV)
                return func.HttpResponse(json.dumps({
                    "message": ERROR_MISSING_CSV,
                    "result": None
                }), status_code=400)

        # Validate required fields for dataset structure
        required_fields = ["dataset_name"]
        missing_fields = []
        if is_multipart:
            dataset_name = req.form.get('dataset_name') if hasattr(req, 'form') else None
            predictors = req.form.get('predictors') if hasattr(req, 'form') else None
            descriptors = req.form.get('descriptors') if hasattr(req, 'form') else None
            if predictors:
                try:
                    predictors = json.loads(predictors) if isinstance(predictors, str) else predictors
                except Exception:
                    return func.HttpResponse(json.dumps({
                        "message": "Predictors must be a JSON list.",
                        "result": None
                    }), status_code=400)
            if descriptors:
                try:
                    descriptors = json.loads(descriptors) if isinstance(descriptors, str) else descriptors
                except Exception:
                    return func.HttpResponse(json.dumps({
                        "message": "Descriptors must be a JSON list.",
                        "result": None
                    }), status_code=400)
        else:
            dataset_name = request_data.get('dataset_name') if request_data else None
            predictors = request_data.get('predictors') if request_data else None
            descriptors = request_data.get('descriptors') if request_data else None
        if not dataset_name:
            missing_fields.append("dataset_name")
        # At least one of predictors or descriptors must be provided
        if not predictors and not descriptors:
            return func.HttpResponse(json.dumps({
                "message": "At least one of 'predictors' or 'descriptors' must be provided.",
                "result": None
            }), status_code=400)
        if predictors and (not isinstance(predictors, list) or not all(isinstance(x, str) for x in predictors)):
            return func.HttpResponse(json.dumps({
                "message": "Predictors must be a list of strings.",
                "result": None
            }), status_code=400)
        if descriptors and (not isinstance(descriptors, list) or not all(isinstance(x, str) for x in descriptors)):
            return func.HttpResponse(json.dumps({
                "message": "Descriptors must be a list of strings.",
                "result": None
            }), status_code=400)

        # Require user to specify the date field in their CSV
        date_field = None
        if is_multipart:
            date_field = req.form.get('date_field') if hasattr(req, 'form') else None
        else:
            date_field = request_data.get('date_field') if request_data else None
        if not date_field:
            return func.HttpResponse(json.dumps({
                "message": "Missing required parameter: 'date_field'. Please specify the name of the date column in your CSV.",
                "result": None
            }), status_code=400)

        logging.info(f"Ingest request: instance_id={instance_id}, dataset_name={dataset_name}, predictors={predictors}, descriptors={descriptors}, date_field={date_field}, csv_url={csv_url}")
        import time
        start_time = time.time()

        # Load blob data to retrieve RSS URL with retry
        try:
            blob_data = retry_with_backoff(
                lambda: load_from_blob_storage(instance_id),
                exceptions=(RuntimeError, ),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            logging.info(f"Loaded blob data for instance_id={instance_id} in {time.time() - start_time:.2f} seconds")
            json_data = json.loads(blob_data)
            rss_url = json_data.get("rss_url")
            if not rss_url:
                logging.error("RSS feed URL not set in the blob. User must POST to /rss before using /ingest.")
                return func.HttpResponse(json.dumps({
                    "message": "RSS feed URL not set. Please POST to /rss with your instance_id and rss_url before using /ingest.",
                    "result": None
                }), status_code=400)
            # Load the daily_dataset from /rss if it exists
            daily_dataset = json_data.get("daily_dataset", [])
        except Exception as e:
            logging.error(f"Failed to load blob or retrieve RSS URL: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to load blob or retrieve RSS URL.",
                "result": None
            }), status_code=500)
        logging.info(f"Blob load and JSON parse complete in {time.time() - start_time:.2f} seconds")

        # Check for dataset overwrite on POST, and prepare for replacement on PUT
        dataset_exists = False
        dataset_dates = set()
        for day in daily_dataset:
            for ds in day.get('datasets', []):
                if ds.get('dataset_name') == dataset_name:
                    dataset_exists = True
                    dataset_dates.add(pd.to_datetime(day['date']).strftime('%Y-%m-%d'))
        if req.method == "POST":
            if dataset_exists:
                logging.error(f"Dataset with name '{dataset_name}' already exists. Use PUT to replace it.")
                return func.HttpResponse(json.dumps({
                    "message": f"Dataset with name '{dataset_name}' already exists. Use PUT to replace it.",
                    "result": None
                }), status_code=409)
        elif req.method == "PUT":
            if dataset_exists:
                # Remove all datasets with this name from daily_dataset
                for day in daily_dataset:
                    if 'datasets' in day:
                        before = len(day['datasets'])
                        day['datasets'] = [ds for ds in day['datasets'] if ds.get('dataset_name') != dataset_name]
                        after = len(day['datasets'])
                        if before != after:
                            logging.info(f"Removed {before - after} entries for dataset_name '{dataset_name}' on date {day.get('date')}")
                # Remove any day that now has no datasets and no other keys except 'date'
                daily_dataset[:] = [day for day in daily_dataset if day.get('datasets') or any(k for k in day if k not in ('date', 'datasets'))]
                logging.info(f"All previous entries for dataset_name '{dataset_name}' removed before replacement.")
            else:
                logging.info(f"No existing dataset with name '{dataset_name}' found. PUT will add as new.")

        # Fetch or use CSV data
        if csv_data is None:
            # Fetch CSV data from URL with retry
            try:
                def fetch_csv():
                    response = requests.get(csv_url, timeout=10)
                    response.raise_for_status()
                    return response.content.decode('utf-8')
                csv_data = retry_with_backoff(
                    fetch_csv,
                    exceptions=(requests.RequestException,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )()
                logging.info(f"Fetched CSV data from URL in {time.time() - start_time:.2f} seconds")
            except Exception as e:
                logging.error(f"Failed to fetch CSV from URL: {e}", exc_info=True)
                return func.HttpResponse(json.dumps({
                    "message": "Failed to fetch CSV from URL.",
                    "result": None
                }), status_code=400)

        # Parse CSV (using StringIO to wrap the string as a file-like object)
        try:
            csv_file_like = io.StringIO(csv_data)
            downloads_df = parse_csv(csv_file_like)
            logging.info(f"Parsed CSV into DataFrame in {time.time() - start_time:.2f} seconds. DataFrame shape: {downloads_df.shape}")
        except Exception as e:
            logging.error(f"Failed to parse CSV: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to parse CSV file.",
                "result": None
            }), status_code=400)

        # Check that the date_field exists in the DataFrame
        if date_field not in downloads_df.columns:
            return func.HttpResponse(json.dumps({
                "message": f"The specified date_field '{date_field}' was not found in the CSV columns.",
                "result": None
            }), status_code=400)

        # Check for duplicate dates before renaming
        if downloads_df[date_field].duplicated().any():
            dupes = downloads_df[downloads_df[date_field].duplicated()][date_field].unique()
            return func.HttpResponse(json.dumps({
                "message": f"Duplicate dates found in the date_field '{date_field}': {list(dupes)}. Please ensure all dates are unique before uploading.",
                "result": None
            }), status_code=400)

        # Rename the date_field to 'Date' for consistency
        downloads_df = downloads_df.rename(columns={date_field: 'Date'})

        # Normalize the 'Date' column to Europe/London local time (with DST), matching /rss logic
        downloads_df['Date'] = normalize_to_london_date(downloads_df['Date'])
        logging.info(f"Date normalization complete in {time.time() - start_time:.2f} seconds")
        if downloads_df['Date'].isnull().any():
            return func.HttpResponse(json.dumps({
                "message": "Some dates could not be parsed or converted to local time. Please check your date values.",
                "result": None
            }), status_code=400)

        # Check that all predictors/descriptors exist in the DataFrame, suggest close matches if not
        import difflib
        csv_cols = set(downloads_df.columns)
        missing_predictors = [p for p in predictors or [] if p not in csv_cols]
        missing_descriptors = [d for d in descriptors or [] if d not in csv_cols]
        error_msgs = []
        for missing in missing_predictors:
            suggestion = difflib.get_close_matches(missing, csv_cols, n=1)
            if suggestion:
                error_msgs.append(f"Predictor '{missing}' not found. Did you mean '{suggestion[0]}'?")
            else:
                error_msgs.append(f"Predictor '{missing}' not found in CSV columns.")
        for missing in missing_descriptors:
            suggestion = difflib.get_close_matches(missing, csv_cols, n=1)
            if suggestion:
                error_msgs.append(f"Descriptor '{missing}' not found. Did you mean '{suggestion[0]}'?")
            else:
                error_msgs.append(f"Descriptor '{missing}' not found in CSV columns.")
        if error_msgs:
            return func.HttpResponse(json.dumps({
                "message": " ".join(error_msgs),
                "result": None
            }), status_code=400)

        # Remove RSS feed parsing from /ingest, use daily_dataset from blob (set by /rss)
        episode_data = None  # Not used in this version

        # Convert to JSON and prepare final blob
        try:
            # Convert 'Date' column to UK local time and add timezone indicator
            local_dt = downloads_df['Date'].dt.tz_convert('Europe/London')
            downloads_df['Date'] = local_dt.dt.strftime('%Y-%m-%dT%H:%M:%S')
            # Add a new column for timezone indicator (BST/GMT)
            downloads_df['timezone'] = local_dt.dt.strftime('%Z')
            logging.info(f"Date formatting and timezone column added in {time.time() - start_time:.2f} seconds")

            # Define predictors and descriptors
            dataset_name = dataset_name  # Already validated
            predictors = predictors or []  # Ensure predictors is a list
            descriptors = descriptors or []  # Ensure descriptors is a list

            # Build structured output for each row (vectorized)
            downloads_df['date'] = pd.to_datetime(downloads_df['Date']).dt.strftime('%Y-%m-%d')
            predictors_df = downloads_df[predictors] if predictors else pd.DataFrame(index=downloads_df.index)
            descriptors_df = downloads_df[descriptors] if descriptors else pd.DataFrame(index=downloads_df.index)
            downloads_df['predictors'] = predictors_df.to_dict(orient='records') if not predictors_df.empty else [{} for _ in range(len(downloads_df))]
            downloads_df['descriptors'] = descriptors_df.to_dict(orient='records') if not descriptors_df.empty else [{} for _ in range(len(downloads_df))]
            downloads_df['dataset_name'] = dataset_name
            downloads_df['dataset_entry'] = downloads_df.apply(lambda row: {
                'dataset_name': row['dataset_name'],
                'predictors': row['predictors'],
                'descriptors': row['descriptors']
            }, axis=1)
            # Group by date and aggregate dataset entries
            grouped = downloads_df.groupby('date')['dataset_entry'].apply(list).reset_index()
            # Merge with daily_dataset if present
            daily_dataset_by_date = {pd.to_datetime(d['date']).strftime('%Y-%m-%d'): d for d in daily_dataset} if daily_dataset else {}
            for _, row in grouped.iterrows():
                date = row['date']
                if date in daily_dataset_by_date:
                    existing_datasets = daily_dataset_by_date[date].setdefault('datasets', [])
                    for ds in row['dataset_entry']:
                        if req.method == "PUT":
                            if ds.get('dataset_name') in [eds.get('dataset_name') for eds in existing_datasets]:
                                continue
                        if ds not in existing_datasets:
                            existing_datasets.append(ds)
                else:
                    # Copy all RSS/time series fields from the daily_dataset (if present)
                    rss_day = next((d for d in daily_dataset if pd.to_datetime(d['date']).strftime('%Y-%m-%d') == date), None) if daily_dataset else None
                    day_entry = {'date': date, 'datasets': row['dataset_entry']}
                    # Ensure all RSS properties are present for new dates
                    if rss_day:
                        for k, v in rss_day.items():
                            if k not in ('date', 'datasets'):
                                day_entry[k] = v
                    else:
                        # If no RSS day, add default properties
                        day_entry.update(get_default_rss_day_properties())
                    daily_dataset_by_date[date] = day_entry
            merged_structured_rows = [daily_dataset_by_date[date] for date in sorted(daily_dataset_by_date.keys())]
            json_data['data'] = merged_structured_rows
            logging.info(f"Structured output construction and merge complete. Structured rows: {len(merged_structured_rows)}")

            if csv_url:
                json_data["csv_url"] = csv_url
        except Exception as e:
            logging.error(f"Failed to convert results to JSON: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to convert results to JSON.",
                "result": None
            }), status_code=500)

        # Save updated blob data with retry
        try:
            retry_with_backoff(
                lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                exceptions=(RuntimeError, ),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            logging.info(f"Saved updated blob data in {time.time() - start_time:.2f} seconds")
        except Exception as e:
            logging.error(f"Failed to save updated blob data: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to save updated blob data.",
                "result": None
            }), status_code=500)

        # Return the data table in a response
        try:
            # Ensure potential_missing_episodes dates match the output format and timezone
            if 'Date' in downloads_df.columns:
                downloads_df['Date'] = pd.to_datetime(downloads_df['Date'])
                if downloads_df['Date'].dt.tz is None or str(downloads_df['Date'].dt.tz) == 'None':
                    downloads_df['Date'] = downloads_df['Date'].dt.tz_localize('UTC').dt.tz_convert('Europe/London')
                else:
                    downloads_df['Date'] = downloads_df['Date'].dt.tz_convert('Europe/London')
                downloads_df['Date'] = downloads_df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            logging.info(f"Response preparation complete in {time.time() - start_time:.2f} seconds")
            response = {
                "message": "Data processed successfully.",
                "result": {
                    "instance_id": instance_id,
                    "data": json_data["data"],
                }
            }
            return func.HttpResponse(
                json.dumps(response),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.error(f"Error preparing response: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Error preparing response.",
                "result": None
            }), status_code=500)

    except ValueError as ve:
        logging.error(str(ve), exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": str(ve),
            "result": None
        }), status_code=400)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "An unexpected error occurred.",
            "result": None
        }), status_code=500)
