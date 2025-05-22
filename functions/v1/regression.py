# regression.py
# Ridge regression endpoint for podcast analytics (Azure Functions)
# Author: [Your Name] | Last updated: 2025-05-22

import logging
import azure.functions as func
import json
import pandas as pd
import numpy as np
from typing import Optional
from utils.azure_blob import load_from_blob_storage
from utils.retry import retry_with_backoff
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import RFECV
from sklearn.preprocessing import StandardScaler
from utils.seasonality import add_time_series_features


def regression(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to perform ridge regression on ingested podcast data.
    Expects: POST with JSON {instance_id, target_col (optional)}
    Returns: Regression results or error message.
    """
    logging.info("Received request for ridge regression analysis.")

    try:
        # --- Request validation ---
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
        target_col: str = request_data.get('target_col', 'Downloads')
        if not instance_id:
            return func.HttpResponse(json.dumps({
                "message": "Missing instance_id.",
                "result": None
            }), status_code=400)

        # --- Load and flatten data ---
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
                    "message": "No data found for regression.",
                    "result": None
                }), status_code=404)
        except Exception as e:
            logging.error(f"Failed to load blob data: {e}", exc_info=True)
            return func.HttpResponse(json.dumps({
                "message": "Failed to load blob data.",
                "result": None
            }), status_code=500)

        # Flatten nested daily_dataset/datasets structure
        flat_rows = []
        for day in data:
            date = day.get('date') or day.get('Date')
            timezone = day.get('timezone')
            # Collect all top-level properties except 'date', 'datasets', and any obvious non-feature fields
            top_level_props = {k: v for k, v in day.items() if k not in ('date', 'Date', 'datasets', 'Episode_Titles', 'Clustered_Episode_Titles', 'episode_titles', 'clustered_episode_titles', 'timezone')}
            for ds in day.get('datasets', []):
                dataset_name = ds.get('dataset_name')
                row = {'Date': date, 'timezone': timezone, 'dataset_name': dataset_name}
                # Add all top-level day properties (e.g. episodes_released, etc.)
                for k, v in top_level_props.items():
                    # Only add if not already present in predictors/descriptors
                    if k not in row:
                        row[k] = v
                for k, v in (ds.get('predictors', {}) or {}).items():
                    row[f'{dataset_name}-{k}'] = v
                for k, v in (ds.get('descriptors', {}) or {}).items():
                    row[f'{dataset_name}-{k}'] = v
                flat_rows.append(row)
        if not flat_rows:
            return func.HttpResponse(json.dumps({
                "message": "No datasets found to run regression.",
                "result": None
            }), status_code=400)
        df = pd.DataFrame(flat_rows)
        logging.info(f"All columns after flattening: {list(df.columns)}")

        # --- Feature engineering ---
        df = add_time_series_features(df, date_col='Date')
        exclude_cols = [target_col, 'Date', 'timezone', 'Episode_Titles', 'Clustered_Episode_Titles']
        predictors = [
            col for col in df.columns
            if col not in exclude_cols
            and not isinstance(df[col].iloc[0], (list, dict))
            and pd.api.types.is_numeric_dtype(df[col])
        ]
        logging.info(f"Initial columns: {list(df.columns)}")
        logging.info(f"Predictors after exclusion: {predictors}")
        logging.info(f"DataFrame shape before dropna: {df.shape}")

        # --- Feature Engineering: Add lagged/rolling/expanding features for all predictors, descriptors, and top-level numeric day properties ---
        feature_base_cols = set(df.columns) - {'Date', 'timezone', 'dataset_name'}
        new_cols = {}
        for col in feature_base_cols:
            if pd.api.types.is_numeric_dtype(df[col]):
                for lag in [1, 7, 14]:
                    new_cols[f'{col}_lag_{lag}'] = df[col].shift(lag)
                new_cols[f'{col}_rolling_mean_7'] = df[col].shift(1).rolling(window=7, min_periods=1).mean()
                new_cols[f'{col}_rolling_min_7'] = df[col].shift(1).rolling(window=7, min_periods=1).min()
                new_cols[f'{col}_rolling_max_7'] = df[col].shift(1).rolling(window=7, min_periods=1).max()
                new_cols[f'{col}_rolling_median_7'] = df[col].shift(1).rolling(window=7, min_periods=1).median()
                new_cols[f'{col}_expanding_mean'] = df[col].expanding().mean().shift(1)
        if new_cols:
            df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
        logging.info(f"All columns after feature engineering: {list(df.columns)}")

        # Remove spike_cluster one-hot columns from predictors and DataFrame
        spike_onehot_cols = [col for col in df.columns if col.startswith('spike_cluster_')]
        if spike_onehot_cols:
            df = df.drop(columns=spike_onehot_cols)
        predictors = [col for col in predictors if not col.startswith('spike_cluster_')]

        # Remove mathematically redundant features to prevent leakage
        if f'{target_col}_lag_1' in predictors and f'{target_col}_diff_1' in predictors:
            predictors.remove(f'{target_col}_diff_1')
        if f'{target_col}_lag_1' in predictors and f'{target_col}_pct_change_1' in predictors:
            predictors.remove(f'{target_col}_pct_change_1')
        predictors = [
            col for col in predictors
            if not col.startswith('spike_cluster_')
            and col != 'is_spike'
            and col != 'potential_missing_episode'
        ]
        predictors = [col for col in predictors if col != 'deduced_episodes_released']

        # Shift rolling predictors to prevent data leakage
        for col in predictors:
            if col.startswith('rolling_'):
                df[col] = df[col].shift(1)

        # Remove predictors with zero variance
        predictors = [col for col in predictors if df[col].nunique() > 1]

        # --- Logging for dropped features and dropped rows ---
        missing_counts = df[predictors + [target_col]].isnull().sum()
        for col, count in missing_counts.items():
            if count > 0:
                logging.info(f"Column '{col}' has {count} missing values before dropna.")
        dropped_rows = df[df[predictors + [target_col]].isnull().any(axis=1)]
        logging.info(f"Number of rows to be dropped due to missing values: {len(dropped_rows)}")
        if not dropped_rows.empty:
            logging.info(f"First 5 dropped rows (index): {dropped_rows.index[:5].tolist()}")

        # Drop rows with missing values in predictors or target
        df = df.dropna(subset=predictors + [target_col])
        logging.info(f"Predictors after dropna: {predictors}")
        logging.info(f"DataFrame shape after dropna: {df.shape}")
        logging.info(f"First 5 rows of X: {df[predictors].head().to_dict()}")
        logging.info(f"First 5 rows of y: {df[target_col].head().tolist()}")

        # --- Ensure more features are included if needed ---
        spike_features = [col for col in predictors if col.startswith('spike_cluster_')]
        non_spike_features = [col for col in predictors if not col.startswith('spike_cluster_')]
        if len(non_spike_features) == 0:
            for fallback in [f'{target_col}_lag_1', f'{target_col}_lag_7', f'rolling_mean']:
                if fallback in df.columns and fallback not in predictors:
                    predictors.append(fallback)
        if 'spike_cluster' in df.columns:
            df = df.drop(columns=['spike_cluster'])
            if 'spike_cluster' in predictors:
                predictors.remove('spike_cluster')
        spike_onehot_cols = [col for col in df.columns if col.startswith('spike_cluster_')]
        for col in spike_onehot_cols:
            df[col] = df[col].fillna(0).astype(int)
            if col not in predictors:
                predictors.append(col)

        # --- Prepare data for regression ---
        X = df[predictors]
        y = df[target_col]
        logging.info(f"Columns prepared for regression (X): {list(X.columns)}")
        for col in X.columns:
            if pd.api.types.is_bool_dtype(X[col]):
                X.loc[:, col] = X[col].astype(int)
        for col in X.columns:
            if pd.api.types.is_object_dtype(X[col]) and not pd.api.types.is_bool_dtype(X[col]):
                X = pd.get_dummies(X, columns=[col], drop_first=True)

        # Remove highly collinear predictors
        corr_matrix = X.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
        X = X.drop(columns=to_drop)
        dropped_features = [column for column in upper.columns if any(upper[column] > 0.95)]
        if dropped_features:
            logging.info(f"Dropped features due to high collinearity: {dropped_features}")

        # --- Feature selection ---
        model = Ridge(alpha=1.0)
        rfecv = RFECV(estimator=model, step=1, cv=5, scoring='r2', min_features_to_select=1)
        rfecv.fit(X, y)
        selected_features = list(X.columns[rfecv.support_])
        X = X[selected_features]
        feature_ranking = dict(zip(X.columns, rfecv.ranking_))
        logging.info(f"RFECV feature ranking (1=selected): {feature_ranking}")
        rfecv_dropped = [col for col in X.columns if col not in selected_features]
        if rfecv_dropped:
            logging.info(f"Dropped features by RFECV: {rfecv_dropped}")
        logging.info(f"Selected features after RFECV: {selected_features}")

        # --- Model training and evaluation ---
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        alphas = np.logspace(-3, 3, 20)
        ridge_cv = RidgeCV(alphas=alphas, cv=5, scoring='r2')
        ridge_cv.fit(X_scaled, y)
        best_alpha = ridge_cv.alpha_
        logging.info(f"Best alpha from RidgeCV: {best_alpha}")
        y_pred_all = ridge_cv.predict(X_scaled)
        residuals = y - y_pred_all
        std_residuals = (residuals - residuals.mean()) / residuals.std()
        mask = std_residuals.abs() <= 3
        X_scaled = X_scaled[mask]
        y = y[mask]
        if 'Date' in df.columns:
            df_masked = df[mask].copy()
            df_masked['Date'] = pd.to_datetime(df_masked['Date'])
            df_masked = df_masked.sort_values('Date')
            n_train = int(len(df_masked) * 0.8)
            X_train = X_scaled[:n_train]
            X_test = X_scaled[n_train:]
            y_train = y.iloc[:n_train]
            y_test = y.iloc[n_train:]
            logging.info(f"Train set last date: {df_masked['Date'].iloc[n_train-1]}")
            logging.info(f"Test set first date: {df_masked['Date'].iloc[n_train]}")
        else:
            X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, shuffle=False)
        logging.info(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
        model = Ridge(alpha=best_alpha)
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)
        coefs = dict(zip(X.columns, model.coef_))
        intercept = model.intercept_
        y_pred = model.predict(X_test)
        logging.info(f"Model coefficients: {coefs}")
        logging.info(f"Model intercept: {intercept}")
        logging.info(f"Predictions: {y_pred.tolist()}")
        logging.info(f"Actuals: {y_test.tolist()}")

        # --- Response ---
        result = {
            "message": "Regression analysis completed successfully.",
            "result": {
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
        }
        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.error(f"Unexpected error in regression endpoint: {e}", exc_info=True)
        return func.HttpResponse(json.dumps({
            "message": "An unexpected error occurred in regression.",
            "result": None
        }), status_code=500)
