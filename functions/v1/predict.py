import azure.functions as func
import logging
import json
import pandas as pd
import numpy as np
from typing import Optional
from utils.azure_blob import load_from_blob_storage
from utils.retry import retry_with_backoff
import joblib
import io

def predict(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to predict the next 60 days using the saved regression model.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with predictions or error message.
    """
    logging.info("Received request for prediction endpoint.")
    try:
        if req.method != "POST":
            return func.HttpResponse(json.dumps({
                "message": "Method not allowed.",
                "result": None
            }), status_code=405)

        try:
            request_data = req.get_json()
        except ValueError:
            return func.HttpResponse(json.dumps({
                "message": "Invalid JSON body",
                "result": None
            }), status_code=400)

        instance_id: Optional[str] = request_data.get('instance_id')
        if not instance_id:
            return func.HttpResponse(json.dumps({
                "message": "Missing instance_id.",
                "result": None
            }), status_code=400)

        # Load model from blob storage
        model_blob_name = f"{instance_id}_ridge_model.joblib"
        try:
            model_bytes = retry_with_backoff(
                lambda: load_from_blob_storage(model_blob_name, binary=True),
                exceptions=(RuntimeError,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            buffer = io.BytesIO(model_bytes)
            model_artifact = joblib.load(buffer)
            model = model_artifact['model']
            scaler = model_artifact['scaler']
            features = model_artifact['features']
            target_col = model_artifact['target']
        except Exception as e:
            logging.error(f"Failed to load model: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to load model.",
                "result": None
            }), status_code=500)

        # Load the latest data
        try:
            blob_data = retry_with_backoff(
                lambda: load_from_blob_storage(instance_id),
                exceptions=(RuntimeError,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            json_data = json.loads(blob_data)
            data = json_data.get("data")
            if not data:
                return func.HttpResponse(json.dumps({
                    "message": "No data found for prediction.",
                    "result": None
                }), status_code=404)
        except Exception as e:
            logging.error(f"Failed to load blob data: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to load blob data.",
                "result": None
            }), status_code=500)

        df = pd.DataFrame(data)
        # Ensure features are present
        for col in features:
            if col not in df.columns:
                df[col] = 0
        # Predict next 60 days
        predictions = []
        # Keep a rolling window of the last N days for lag/rolling features
        history = df.copy()
        for i in range(60):
            # Compute date for prediction
            last_row = history.iloc[-1].copy()
            pred_date = pd.to_datetime(last_row['Date']) + pd.Timedelta(days=1)
            # Build new row for prediction
            new_row = last_row.copy()
            new_row['Date'] = pred_date
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
            # Store prediction
            predictions.append({
                'date': pred_date.strftime('%Y-%m-%d'),
                'prediction': float(y_pred)
            })
            # Update target for next iteration
            new_row[target_col] = y_pred
            # Append new_row to history for next iteration
            history = pd.concat([history, pd.DataFrame([new_row])], ignore_index=True)
        return func.HttpResponse(json.dumps({
            "message": "Prediction completed successfully.",
            "result": predictions
        }), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Unexpected error in predict endpoint: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "An unexpected error occurred in predict.",
            "result": None
        }), status_code=500)
