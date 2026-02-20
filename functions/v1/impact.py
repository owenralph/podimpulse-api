import logging
import azure.functions as func
from utils.regression import load_json_from_blob, add_lagged_episode_release_columns, summarize_impact_results
import pandas as pd
import numpy as np
import json
from typing import Optional
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import StandardScaler

def impact(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to calculate the impact of episode releases using regression analysis.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with regression results or error message.
    """
    logging.debug("[impact] Received request to calculate episode impact using regression.")
    logging.info("Received request to calculate episode impact using regression.")

    try:
        # Validate HTTP method
        if req.method != "GET":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(
                "Invalid HTTP method. Only GET requests are allowed.",
                status_code=405
            )

        podcast_id: Optional[str] = req.route_params.get("podcast_id")
        if not podcast_id:
            return func.HttpResponse("Missing podcast_id in path.", status_code=400)

        # Retrieve JSON from Blob Storage with retry
        try:
            json_data = load_json_from_blob(podcast_id)
        except Exception as e:
            error_message = f"Error retrieving data for podcast_id {podcast_id}: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse("Error retrieving data from storage.", status_code=404)

        # Parse JSON into DataFrame
        try:
            payload = json.loads(json_data)
            data = payload.get("data", [])
            if not data:
                return func.HttpResponse("No data found for impact analysis.", status_code=404)
            df = pd.DataFrame(data)
            df["Date"] = pd.to_datetime(df["Date"])
            df.sort_values("Date", inplace=True)
        except Exception as e:
            error_message = f"Error parsing dataset: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse("Error parsing dataset.", status_code=400)

        # Add columns for episodes released in the past 0–7 days
        try:
            max_days = 7
            df = add_lagged_episode_release_columns(df, max_days=max_days)
        except Exception as e:
            logging.error(f"Error adding lag columns: {e}", exc_info=True)
            return func.HttpResponse("Error preparing features for regression.", status_code=500)

        # Prepare predictors (X) and response (y)
        try:
            predictors = [f"Episodes released today-{i}" for i in range(max_days + 1)]
            # Remove predictors with zero variance
            predictors = [col for col in predictors if df[col].nunique() > 1]
            if not predictors:
                return func.HttpResponse("Not enough variation in features for impact analysis.", status_code=400)
            df = df.dropna(subset=predictors + ['Downloads'])
            X = df[predictors]
            y = df['Downloads']
        except Exception as e:
            logging.error(f"Error preparing predictors/response: {e}", exc_info=True)
            return func.HttpResponse("Error preparing regression data.", status_code=500)

        if len(X) < 3:
            return func.HttpResponse("Not enough data points for impact analysis.", status_code=400)

        # Feature scaling
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Hyperparameter tuning for Ridge (RidgeCV)
        alphas = np.logspace(-3, 3, 20)
        ridge_cv = RidgeCV(alphas=alphas, cv=min(5, len(X)), scoring='r2')
        ridge_cv.fit(X_scaled, y)
        best_alpha = ridge_cv.alpha_

        # Outlier detection and removal (standardized residuals > 3)
        y_pred_all = ridge_cv.predict(X_scaled)
        residuals = y - y_pred_all
        std_residuals = (residuals - residuals.mean()) / residuals.std()
        mask = std_residuals.abs() <= 3
        X_scaled = X_scaled[mask]
        y = y[mask]
        df_masked = df[mask].copy()

        # Time-aware train/test split
        df_masked['Date'] = pd.to_datetime(df_masked['Date'])
        df_masked = df_masked.sort_values('Date')
        split_idx = int(len(df_masked) * 0.8)
        X_train = X_scaled[:split_idx]
        X_test = X_scaled[split_idx:]
        y_train = y.iloc[:split_idx]
        y_test = y.iloc[split_idx:]
        if len(X_train) == 0 or len(X_test) == 0:
            return func.HttpResponse("Not enough data to create train/test split.", status_code=400)

        # Fit final Ridge model with best alpha
        model = RidgeCV(alphas=[best_alpha], cv=None)
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)
        coefs = dict(zip(X.columns, model.coef_))
        intercept = model.intercept_
        y_pred = model.predict(X_test)

        # Analyze results (keep original logic for impact summary)
        try:
            results = []
            for i, predictor in enumerate(predictors):
                coef = coefs.get(predictor, 0.0)
                # Ridge does not provide p-values; just report all coefficients
                results.append({
                    'day_offset': i,
                    'impact': coef,
                    'p_value': None
                })
        except Exception as e:
            logging.error(f"Error analyzing regression results: {e}", exc_info=True)
            return func.HttpResponse("Error analyzing regression results.", status_code=500)

        # Summarize results
        try:
            days_of_impact, average_impact, impact_per_day = summarize_impact_results(results)
        except Exception as e:
            logging.error(f"Error summarizing regression results: {e}", exc_info=True)
            return func.HttpResponse("Error summarizing regression results.", status_code=500)

        # Prepare response
        try:
            response = {
                'message': 'Impact analysis completed successfully.',
                'result': {
                    'days_of_impact': days_of_impact,
                    'average_impact': average_impact,
                    'impact_per_day': impact_per_day,
                    'score': score,
                    'best_alpha': best_alpha,
                    'n_train': len(X_train),
                    'n_test': len(X_test),
                    'predictions': y_pred.tolist(),
                    'actuals': y_test.tolist(),
                    'intercept': float(intercept),
                    'coefficients': coefs,
                    'selected_features': predictors
                }
            }
            return func.HttpResponse(
                json.dumps(response),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.error(f"Error preparing response: {e}", exc_info=True)
            return func.HttpResponse("Error preparing response.", status_code=500)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            "An unexpected error occurred.",
            status_code=500
        )
