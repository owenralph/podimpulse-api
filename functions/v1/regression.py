import logging
import azure.functions as func
import json
import pandas as pd
import numpy as np
from typing import Optional
from utils.azure_blob import load_from_blob_storage
from utils.retry import retry_with_backoff
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.feature_selection import RFECV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV

def regression(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to perform ridge regression on ingested podcast data.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with regression results or error message.
    """
    logging.info("Received request for ridge regression analysis.")

    try:
        if req.method != "POST":
            return func.HttpResponse("Method not allowed.", status_code=405)

        try:
            request_data = req.get_json()
        except ValueError:
            return func.HttpResponse("Invalid JSON body", status_code=400)

        instance_id: Optional[str] = request_data.get('instance_id')
        target_col: str = request_data.get('target_col', 'Downloads')

        if not instance_id:
            return func.HttpResponse("Missing instance_id.", status_code=400)

        # Load blob data with retry
        try:
            blob_data = retry_with_backoff(
                lambda: load_from_blob_storage(instance_id),
                exceptions=(RuntimeError, ),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
            json_data = json.loads(blob_data)
            data = json_data.get("data")
            if not data:
                return func.HttpResponse("No data found for regression.", status_code=404)
        except Exception as e:
            logging.error(f"Failed to load blob data: {e}", exc_info=True)
            return func.HttpResponse("Failed to load blob data.", status_code=500)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Select predictors (seasonality, cluster, missing, etc.)
        predictors = [
            'day_of_week_sin', 'day_of_week_cos', 'month_sin', 'month_cos',
        ]
        if 'Cluster' in df.columns:
            predictors.append('Cluster')
        if 'Missing' in df.columns:
            predictors.append('Missing')
        # Remove predictors with zero variance (constant columns)
        predictors = [col for col in predictors if df[col].nunique() > 1]
        # Drop rows with missing values in predictors or target
        df = df.dropna(subset=predictors + [target_col])

        X = df[predictors]
        y = df[target_col]

        # One-hot encode categorical predictors if present
        if 'Cluster' in predictors:
            X = pd.get_dummies(X, columns=['Cluster'], drop_first=True)
        if 'Missing' in predictors and X['Missing'].dtype == object:
            X['Missing'] = X['Missing'].astype(int)

        # Remove highly collinear predictors (correlation > 0.95)
        corr_matrix = X.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
        X = X.drop(columns=to_drop)

        # Iterative feature selection using RFECV (recursive feature elimination with cross-validation)
        model = Ridge(alpha=1.0)
        rfecv = RFECV(estimator=model, step=1, cv=5, scoring='r2', min_features_to_select=1)
        rfecv.fit(X, y)
        selected_features = list(X.columns[rfecv.support_])
        X = X[selected_features]

        # Feature scaling (standardization)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Hyperparameter tuning for Ridge (RidgeCV)
        alphas = np.logspace(-3, 3, 20)
        ridge_cv = RidgeCV(alphas=alphas, cv=5, scoring='r2')
        ridge_cv.fit(X_scaled, y)
        best_alpha = ridge_cv.alpha_

        # Outlier detection and removal (remove samples with standardized residuals > 3)
        y_pred_all = ridge_cv.predict(X_scaled)
        residuals = y - y_pred_all
        std_residuals = (residuals - residuals.mean()) / residuals.std()
        mask = std_residuals.abs() <= 3
        X_scaled = X_scaled[mask]
        y = y[mask]

        # Time-aware train/test split (if 'Date' column exists)
        if 'Date' in df.columns:
            df_masked = df[mask].copy()
            df_masked['Date'] = pd.to_datetime(df_masked['Date'])
            df_masked = df_masked.sort_values('Date')
            split_idx = int(len(df_masked) * 0.8)
            X_train = X_scaled[:split_idx]
            X_test = X_scaled[split_idx:]
            y_train = y.iloc[:split_idx]
            y_test = y.iloc[split_idx:]
        else:
            X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

        # Fit final Ridge model with best alpha
        model = Ridge(alpha=best_alpha)
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)
        coefs = dict(zip(X.columns, model.coef_))
        intercept = model.intercept_
        y_pred = model.predict(X_test)

        result = {
            "score": score,
            "intercept": intercept,
            "coefficients": coefs,
            "n_train": len(X_train),
            "n_test": len(X_test),
            "selected_features": selected_features,
            "best_alpha": best_alpha,
            "predictions": y_pred.tolist(),
            "actuals": y_test.tolist(),
        }
        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.error(f"Unexpected error in regression endpoint: {e}", exc_info=True)
        return func.HttpResponse("An unexpected error occurred in regression.", status_code=500)
