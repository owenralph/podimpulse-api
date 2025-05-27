import azure.functions as func
import logging
import json
import pandas as pd
import numpy as np
from typing import Optional
from utils.azure_blob import load_from_blob_storage
from utils.retry import retry_with_backoff
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import io
import joblib

def predict(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to provide advanced forecasting and optimization for podcast episodes.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with prediction results or error message.
    """
    logging.debug("[predict] Received request for advanced forecasting and optimization.")
    # Validate HTTP method
    method_error = validate_http_method(req, ["POST", "GET"])
    if method_error:
        return method_error
    podcast_id: Optional[str] = req.route_params.get("podcast_id")
    if not podcast_id:
        return error_response("Missing podcast_id in path.", 400)
    try:
        if req.method == "POST":
            try:
                request_data = req.get_json()
            except ValueError:
                return error_response("Invalid JSON body", 400)
            episodes = request_data.get('episodes')
            release_dates = request_data.get('release_dates', [])
            release_dates_set = set(pd.to_datetime(release_dates).strftime('%Y-%m-%d'))
            if not podcast_id:
                return error_response("Missing podcast_id.", 400)
            if episodes is None and not release_dates:
                auto_count_episodes = True
            else:
                auto_count_episodes = False
            # Load model from blob storage
            model_blob_name = f"{podcast_id}_ridge_model.joblib"
            model_bytes, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(model_blob_name, binary=True),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load model.", 500)
            buffer = io.BytesIO(model_bytes)
            model_artifact = joblib.load(buffer)
            model = model_artifact['model']
            scaler = model_artifact['scaler']
            features = model_artifact['features']
            target_col = model_artifact['target']
            # Load the latest data
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load blob data.", 500)
            json_data = json.loads(blob_data)
            data = json_data.get("data")
            if not data:
                return error_response("No data found for prediction.", 404)
            df = pd.DataFrame(data)
            for col in features:
                if col not in df.columns:
                    df[col] = 0
            predicted_rows = []
            history = df.copy()
            # Track which dates are set as release dates
            used_release_dates = set(release_dates_set)
            # For optimization, store candidate dates and their predicted downloads
            candidate_dates = []
            def _run_forecast(
                history_df, features, scaler, model, target_col, release_dates_set, release_indices=None, forecast_days=60
            ):
                rerun_predicted_rows = []
                for i in range(forecast_days):
                    last_row = history_df.iloc[-1].copy()
                    pred_date = pd.to_datetime(last_row['Date']) + pd.Timedelta(days=1)
                    new_row = last_row.copy()
                    new_row['Date'] = pred_date
                    date_str = pred_date.strftime('%Y-%m-%d')
                    # Set Episodes Released based on release_dates or optimized indices
                    if 'Episodes Released' in new_row:
                        if date_str in release_dates_set or (release_indices and i in release_indices):
                            new_row['Episodes Released'] = 1
                        else:
                            new_row['Episodes Released'] = 0
                    for lag in [1, 7, 14]:
                        lag_col = f'{target_col}_lag_{lag}'
                        if lag_col in new_row:
                            if len(history_df) >= lag:
                                new_row[lag_col] = history_df[target_col].iloc[-lag]
                            else:
                                new_row[lag_col] = np.nan
                    for stat in ['min', 'max', 'median']:
                        colname = f'rolling_{stat}_7'
                        if colname in new_row:
                            window = history_df[target_col].iloc[-7:] if len(history_df) >= 7 else history_df[target_col]
                            new_row[colname] = getattr(window, stat)()
                    if f'{target_col}_expanding_mean' in new_row:
                        new_row[f'{target_col}_expanding_mean'] = history_df[target_col].expanding().mean().iloc[-1]
                    if 'is_weekend' in new_row:
                        new_row['is_weekend'] = pred_date.weekday() >= 5
                    day_of_year = pred_date.timetuple().tm_yday
                    for k in [1, 2]:
                        sin_col = f'fourier_sin_{k}'
                        cos_col = f'fourier_cos_{k}'
                        if sin_col in new_row:
                            new_row[sin_col] = np.sin(2 * np.pi * k * day_of_year / 365.25)
                        if cos_col in new_row:
                            new_row[cos_col] = np.cos(2 * np.pi * k * day_of_year / 365.25)
                    if 'Episodes Released' in new_row:
                        for lag in [1, 2, 3, 7]:
                            lag_col = f'Episodes_Released_lag_{lag}'
                            if lag_col in new_row:
                                if len(history_df) >= lag:
                                    new_row[lag_col] = history_df['Episodes Released'].iloc[-lag]
                                else:
                                    new_row[lag_col] = 0
                        if 'Episodes_Released_rolling_7' in new_row:
                            window = history_df['Episodes Released'].iloc[-7:] if len(history_df) >= 7 else history_df['Episodes Released']
                            new_row['Episodes_Released_rolling_7'] = window.sum()
                    for col in features:
                        if col not in new_row or pd.isnull(new_row[col]):
                            new_row[col] = 0
                    X_input = pd.DataFrame([new_row[features]], columns=features)
                    X_scaled = scaler.transform(X_input)
                    y_pred = model.predict(X_scaled)[0]
                    new_row[target_col] = y_pred
                    rerun_predicted_rows.append(new_row.copy())
                    history_df = pd.concat([history_df, pd.DataFrame([new_row])], ignore_index=True)
                return rerun_predicted_rows
            for i in range(60):
                last_row = history.iloc[-1].copy()
                pred_date = pd.to_datetime(last_row['Date']) + pd.Timedelta(days=1)
                new_row = last_row.copy()
                new_row['Date'] = pred_date
                # Set Episodes Released based on release_dates
                date_str = pred_date.strftime('%Y-%m-%d')
                if 'Episodes Released' in new_row:
                    if date_str in release_dates_set:
                        new_row['Episodes Released'] = 1
                    else:
                        new_row['Episodes Released'] = 0
                # Update lagged target features
                for lag in [1, 7, 14]:
                    lag_col = f'{target_col}_lag_{lag}'
                    if lag_col in new_row:
                        if len(history) >= lag:
                            new_row[lag_col] = history[target_col].iloc[-lag]
                        else:
                            new_row[lag_col] = np.nan
                # Update rolling features
                for stat in ['min', 'max', 'median']:
                    colname = f'rolling_{stat}_7'
                    if colname in new_row:
                        window = history[target_col].iloc[-7:] if len(history) >= 7 else history[target_col]
                        new_row[colname] = getattr(window, stat)()
                # Update expanding mean
                if f'{target_col}_expanding_mean' in new_row:
                    new_row[f'{target_col}_expanding_mean'] = history[target_col].expanding().mean().iloc[-1]
                # Update is_weekend
                if 'is_weekend' in new_row:
                    new_row['is_weekend'] = pred_date.weekday() >= 5
                # Update Fourier terms
                day_of_year = pred_date.timetuple().tm_yday
                for k in [1, 2]:
                    sin_col = f'fourier_sin_{k}'
                    cos_col = f'fourier_cos_{k}'
                    if sin_col in new_row:
                        new_row[sin_col] = np.sin(2 * np.pi * k * day_of_year / 365.25)
                    if cos_col in new_row:
                        new_row[cos_col] = np.cos(2 * np.pi * k * day_of_year / 365.25)
                # Update episode released lags/rolling
                if 'Episodes Released' in new_row:
                    for lag in [1, 2, 3, 7]:
                        lag_col = f'Episodes_Released_lag_{lag}'
                        if lag_col in new_row:
                            if len(history) >= lag:
                                new_row[lag_col] = history['Episodes Released'].iloc[-lag]
                            else:
                                new_row[lag_col] = 0
                    if 'Episodes_Released_rolling_7' in new_row:
                        window = history['Episodes Released'].iloc[-7:] if len(history) >= 7 else history['Episodes Released']
                        new_row['Episodes_Released_rolling_7'] = window.sum()
                # Fill any missing features with 0
                for col in features:
                    if col not in new_row or pd.isnull(new_row[col]):
                        new_row[col] = 0
                # Prepare input for prediction
                X_input = pd.DataFrame([new_row[features]], columns=features)
                X_scaled = scaler.transform(X_input)
                y_pred = model.predict(X_scaled)[0]
                new_row[target_col] = y_pred
                # Optionally, update any additional columns (e.g., timezone, etc.)
                predicted_rows.append(new_row.copy())
                history = pd.concat([history, pd.DataFrame([new_row])], ignore_index=True)
                # If this date is not in release_dates_set, store as candidate for optimization
                if episodes is not None and episodes > len(release_dates_set):
                    if date_str not in release_dates_set:
                        candidate_dates.append((i, date_str, y_pred, new_row.copy()))
            # If auto_count_episodes, count how many days in predicted_rows have Episodes Released==1
            if auto_count_episodes:
                episodes = sum(1 for row in predicted_rows if row.get('Episodes Released', 0) == 1)
            # If episodes > len(release_dates_set), optimize additional release dates
            if episodes is not None and episodes > len(release_dates_set):
                # Find the (episodes - len(release_dates_set)) candidate dates with highest predicted downloads
                n_to_add = episodes - len(release_dates_set)
                # Sort candidate_dates by predicted downloads, descending
                candidate_dates_sorted = sorted(candidate_dates, key=lambda x: x[2], reverse=True)
                # Get the indices of the top n_to_add dates
                indices_to_set = [x[0] for x in candidate_dates_sorted[:n_to_add]]
                # Set Episodes Released=1 for those dates in predicted_rows
                for idx in indices_to_set:
                    predicted_rows[idx]['Episodes Released'] = 1
                optimized_release_indices = set(indices_to_set)
                optimized_release_dates = set(
                    predicted_rows[idx]['Date'].strftime('%Y-%m-%d') if isinstance(predicted_rows[idx]['Date'], pd.Timestamp) else str(predicted_rows[idx]['Date'])
                    for idx in indices_to_set
                )
                # Log optimized release dates
                logging.info(f"Optimized release indices: {sorted(optimized_release_indices)}")
                logging.info(f"Optimized release dates: {sorted(optimized_release_dates)}")
                # Re-run the forecast with the new release schedule
                predicted_rows = _run_forecast(
                    df.copy(), features, scaler, model, target_col, release_dates_set, optimized_release_indices, forecast_days=60
                )
            pred_df = pd.DataFrame(predicted_rows)
            if 'Date' in pred_df.columns:
                if 'timezone' in df.columns:
                    tz = history.iloc[-1].get('timezone', 'UTC')
                    pred_df['timezone'] = tz
                # Always output as ISO8601 UTC
                pred_df['Date'] = pd.to_datetime(pred_df['Date'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            for col in df.columns:
                if col not in pred_df.columns:
                    pred_df[col] = None
            pred_df = pred_df[df.columns]
            result_records = pred_df.to_dict(orient="records")
            total_downloads = float(pred_df[target_col].sum()) if target_col in pred_df.columns else None
            # Log total downloads
            logging.info(f"Total predicted downloads: {total_downloads}")
            response = {
                "message": "Prediction completed successfully.",
                "result": result_records,
                "total_downloads": total_downloads
            }
            if episodes is not None and episodes > len(release_dates_set):
                response["optimized_release_dates"] = sorted(list(optimized_release_dates))
            return json_response(response, 200)
        elif req.method == "GET":
            # Retrieve most recent prediction results (implement as needed)
            return error_response("GET /predict not implemented yet.", 501)
        else:
            return error_response("Method Not Allowed", 405)
    except Exception as e:
        logging.error(f"Unexpected error in predict endpoint: {e}", exc_info=True)
        return error_response("An unexpected error occurred in predict.", 500)
