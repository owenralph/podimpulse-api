import logging
import azure.functions as func
import json
import pandas as pd
import numpy as np
from typing import Optional
from utils.azure_blob import load_from_blob_storage, save_to_blob_storage
from utils.retry import retry_with_backoff
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.feature_selection import RFECV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
import joblib
import io
from utils import validate_http_method, json_response, handle_blob_operation, error_response

def regression(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to perform ridge regression on ingested podcast data.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with regression results or error message.
    """
    logging.debug("[regression] Received request to perform ridge regression on podcast data.")
    logging.info("Received request for ridge regression analysis.")
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
            target_col: str = request_data.get('target_col', 'Downloads')
            # Load blob data with retry
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_from_blob_storage(podcast_id),
                    exceptions=(RuntimeError, ),
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
                return error_response("No data found for regression.", 404)
            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Automatically use all suitable predictors in the DataFrame
            # Exclude the target, lists, date, and timezone columns
            exclude_cols = [target_col, 'Date', 'timezone', 'Episode_Titles', 'Clustered_Episode_Titles']
            predictors = [
                col for col in df.columns
                if col not in exclude_cols
                and not isinstance(df[col].iloc[0], (list, dict))
                and pd.api.types.is_numeric_dtype(df[col])
            ]

            # --- Logging for Debugging ---
            logging.info(f"Initial columns: {list(df.columns)}")
            logging.info(f"Predictors after exclusion: {predictors}")
            logging.info(f"DataFrame shape before dropna: {df.shape}")
            # --- Feature Engineering Enhancements ---
            # Add lagged values of the target variable (e.g., previous 1, 7, 14 days)
            for lag in [1, 7, 14]:
                lag_col = f'{target_col}_lag_{lag}'
                df[lag_col] = df[target_col].shift(lag)
                if lag_col not in predictors:
                    predictors.append(lag_col)
            # Add first difference and percent change of the target
            df[f'{target_col}_diff_1'] = df[target_col].diff(1)
            df[f'{target_col}_pct_change_1'] = df[target_col].pct_change(1)
            predictors += [f'{target_col}_diff_1', f'{target_col}_pct_change_1']
            # Add rolling min, max, median (shifted to avoid leakage)
            for stat in ['min', 'max', 'median']:
                colname = f'rolling_{stat}_7'
                df[colname] = getattr(df[target_col].rolling(window=7, min_periods=1), stat)().shift(1)
                predictors.append(colname)
            # Add expanding mean (cumulative mean up to previous day)
            df[f'{target_col}_expanding_mean'] = df[target_col].expanding().mean().shift(1)
            predictors.append(f'{target_col}_expanding_mean')
            # Add weekend/holiday indicator (UK holidays not implemented, but weekend is)
            if 'Date' in df.columns:
                df['is_weekend'] = pd.to_datetime(df['Date']).dt.weekday >= 5
                predictors.append('is_weekend')
            # Fourier terms for yearly seasonality (first 2 harmonics)
            if 'Date' in df.columns:
                dt = pd.to_datetime(df['Date'])
                day_of_year = dt.dt.dayofyear
                for k in [1, 2]:
                    df[f'fourier_sin_{k}'] = np.sin(2 * np.pi * k * day_of_year / 365.25)
                    df[f'fourier_cos_{k}'] = np.cos(2 * np.pi * k * day_of_year / 365.25)
                    predictors += [f'fourier_sin_{k}', f'fourier_cos_{k}']
            # Add tail predictors for episodes released (lags and rolling sum)
            if 'Episodes Released' in df.columns:
                for lag in [1, 2, 3, 7]:
                    df[f'Episodes_Released_lag_{lag}'] = df['Episodes Released'].shift(lag).fillna(0).astype(int)
                    if f'Episodes_Released_lag_{lag}' not in predictors:
                        predictors.append(f'Episodes_Released_lag_{lag}')
                df['Episodes_Released_rolling_7'] = df['Episodes Released'].shift(1).rolling(window=7, min_periods=1).sum().fillna(0).astype(int)
                if 'Episodes_Released_rolling_7' not in predictors:
                    predictors.append('Episodes_Released_rolling_7')
            # Add interaction features between Episodes Released and its lags/rolling
            if 'Episodes Released' in df.columns:
                for lag in [1, 2, 3, 7]:
                    lag_col = f'Episodes_Released_lag_{lag}'
                    if lag_col in df.columns:
                        inter_col = f'Episodes_Released_x_lag_{lag}'
                        df[inter_col] = df['Episodes Released'] * df[lag_col]
                        predictors.append(inter_col)
                if 'Episodes_Released_rolling_7' in df.columns:
                    inter_col = 'Episodes_Released_x_rolling_7'
                    df[inter_col] = df['Episodes Released'] * df['Episodes_Released_rolling_7']
                    predictors.append(inter_col)
                # Extra interaction features
                if 'is_weekend' in df.columns:
                    inter_col = 'Episodes_Released_x_is_weekend'
                    df[inter_col] = df['Episodes Released'] * df['is_weekend'].astype(int)
                    predictors.append(inter_col)
                # Day of week Fourier features
                day_of_week = pd.to_datetime(df['Date']).dt.weekday if 'Date' in df.columns else None
                if day_of_week is not None:
                    df['day_of_week_sin'] = np.sin(2 * np.pi * day_of_week / 7)
                    df['day_of_week_cos'] = np.cos(2 * np.pi * day_of_week / 7)
                    predictors += ['day_of_week_sin', 'day_of_week_cos']
                    inter_col_sin = 'Episodes_Released_x_day_of_week_sin'
                    inter_col_cos = 'Episodes_Released_x_day_of_week_cos'
                    df[inter_col_sin] = df['Episodes Released'] * df['day_of_week_sin']
                    df[inter_col_cos] = df['Episodes Released'] * df['day_of_week_cos']
                    predictors += [inter_col_sin, inter_col_cos]
            # --- End Feature Engineering Enhancements ---

            # --- Remove spike_cluster one-hot columns from predictors and DataFrame entirely (for extra safety) ---
            spike_onehot_cols = [col for col in df.columns if col.startswith('spike_cluster_')]
            if spike_onehot_cols:
                df = df.drop(columns=spike_onehot_cols)
            predictors = [col for col in predictors if not col.startswith('spike_cluster_')]

            # --- Prevent Target Leakage: Remove mathematically redundant features ---
            # If both lagged and diff features are present, remove one to avoid perfect reconstruction
            # For example, if Downloads_lag_1 and Downloads_diff_1 are both present, remove Downloads_diff_1
            if f'{target_col}_lag_1' in predictors and f'{target_col}_diff_1' in predictors:
                predictors.remove(f'{target_col}_diff_1')
            # Also, if both lag and pct_change are present, remove pct_change (optional, for safety)
            if f'{target_col}_lag_1' in predictors and f'{target_col}_pct_change_1' in predictors:
                predictors.remove(f'{target_col}_pct_change_1')

            # Remove is_spike, spike_cluster one-hot, and potential_missing_episode columns from predictors to prevent data leakage
            predictors = [
                col for col in predictors
                if not col.startswith('spike_cluster_')
                and col != 'is_spike'
                and col != 'potential_missing_episode'
            ]
            # Also remove deduced_episodes_released if present
            predictors = [col for col in predictors if col != 'deduced_episodes_released']

            # Shift rolling predictors to prevent data leakage (use only past data)
            for col in predictors:
                if col.startswith('rolling_'):
                    df[col] = df[col].shift(1)

            # Remove predictors with zero variance (constant columns)
            predictors = [col for col in predictors if df[col].nunique() > 1]

            # --- Logging for dropped features and dropped rows ---
            # Before dropna, log which predictors have missing values and how many
            missing_counts = df[predictors + [target_col]].isnull().sum()
            for col, count in missing_counts.items():
                if count > 0:
                    logging.info(f"Column '{col}' has {count} missing values before dropna.")
            # Log which rows will be dropped
            dropped_rows = df[df[predictors + [target_col]].isnull().any(axis=1)]
            logging.info(f"Number of rows to be dropped due to missing values: {len(dropped_rows)}")
            if not dropped_rows.empty:
                logging.info(f"First 5 dropped rows (index): {dropped_rows.index[:5].tolist()}")

            # Drop rows with missing values in predictors or target
            df = df.dropna(subset=predictors + [target_col])

            # --- Logging for Debugging ---
            logging.info(f"Predictors after dropna: {predictors}")
            logging.info(f"DataFrame shape after dropna: {df.shape}")
            logging.info(f"First 5 rows of X: {df[predictors].head().to_dict()}")
            logging.info(f"First 5 rows of y: {df[target_col].head().tolist()}")

            # --- Debug: Ensure more features are included ---
            # If only spike_cluster features are selected, force inclusion of key time series features
            spike_features = [col for col in predictors if col.startswith('spike_cluster_')]
            non_spike_features = [col for col in predictors if not col.startswith('spike_cluster_')]
            # If all selected features are spike_cluster, add back lagged target and rolling mean if available
            if len(non_spike_features) == 0:
                for fallback in [f'{target_col}_lag_1', f'{target_col}_lag_7', f'rolling_mean']:
                    if fallback in df.columns and fallback not in predictors:
                        predictors.append(fallback)

            # Remove the original spike_cluster column if present
            if 'spike_cluster' in df.columns:
                df = df.drop(columns=['spike_cluster'])
                if 'spike_cluster' in predictors:
                    predictors.remove('spike_cluster')

            # Ensure all one-hot spike cluster columns are integer type (0/1), not boolean
            spike_onehot_cols = [col for col in df.columns if col.startswith('spike_cluster_')]
            for col in spike_onehot_cols:
                df[col] = df[col].fillna(0).astype(int)
                if col not in predictors:
                    predictors.append(col)

            X = df[predictors]
            y = df[target_col]

            # Convert boolean columns to int using .loc to avoid SettingWithCopyWarning
            for col in X.columns:
                if pd.api.types.is_bool_dtype(X[col]):
                    X.loc[:, col] = X[col].astype(int)
            # One-hot encode categorical predictors if present
            for col in X.columns:
                if pd.api.types.is_object_dtype(X[col]) and not pd.api.types.is_bool_dtype(X[col]):
                    X = pd.get_dummies(X, columns=[col], drop_first=True)

            # Remove highly collinear predictors (correlation > 0.95)
            corr_matrix = X.corr().abs()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
            X = X.drop(columns=to_drop)

            # After collinearity removal, log which features were dropped
            dropped_features = [column for column in upper.columns if any(upper[column] > 0.95)]
            if dropped_features:
                logging.info(f"Dropped features due to high collinearity: {dropped_features}")

            # Iterative feature selection using RFECV (recursive feature elimination with cross-validation)
            model = Ridge(alpha=1.0)
            rfecv = RFECV(estimator=model, step=1, cv=5, scoring='r2', min_features_to_select=1)
            rfecv.fit(X, y)
            selected_features = list(X.columns[rfecv.support_])
            X = X[selected_features]

            # Log feature ranking from RFECV for traceability
            feature_ranking = dict(zip(X.columns, rfecv.ranking_))
            logging.info(f"RFECV feature ranking (1=selected): {feature_ranking}")

            # After RFECV, log which features were dropped
            rfecv_dropped = [col for col in X.columns if col not in selected_features]
            if rfecv_dropped:
                logging.info(f"Dropped features by RFECV: {rfecv_dropped}")

            # --- Logging for Debugging ---
            logging.info(f"Selected features after RFECV: {selected_features}")

            # Feature scaling (standardization)
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            # Hyperparameter tuning for Ridge (RidgeCV)
            alphas = np.logspace(-3, 3, 20)
            ridge_cv = RidgeCV(alphas=alphas, cv=5, scoring='r2')
            ridge_cv.fit(X_scaled, y)
            best_alpha = ridge_cv.alpha_

            # --- Logging for Debugging ---
            logging.info(f"Best alpha from RidgeCV: {best_alpha}")

            # Outlier detection and removal (remove samples with standardized residuals > 3)
            y_pred_all = ridge_cv.predict(X_scaled)
            residuals = y - y_pred_all
            std_residuals = (residuals - residuals.mean()) / residuals.std()
            mask = std_residuals.abs() <= 3
            X_scaled = X_scaled[mask]
            y = y[mask]

            # Time-aware train/test split (chronological, not random)
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

            # --- Logging for Debugging ---
            logging.info(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

            # Fit final Ridge model with best alpha
            model = Ridge(alpha=best_alpha)
            model.fit(X_train, y_train)
            score = model.score(X_test, y_test)
            coefs = dict(zip(X.columns, model.coef_))
            intercept = model.intercept_
            y_pred = model.predict(X_test)

            # --- Save trained model to Azure Blob Storage ---
            try:
                # Serialize model, scaler, and feature list to bytes using BytesIO
                model_artifact = {
                    'model': model,
                    'scaler': scaler,
                    'features': list(X.columns),
                    'target': target_col,
                    'podcast_id': podcast_id,
                    'timestamp': pd.Timestamp.now().isoformat()
                }
                buffer = io.BytesIO()
                joblib.dump(model_artifact, buffer)
                buffer.seek(0)
                model_blob_name = f"{podcast_id}_ridge_model.joblib"
                retry_with_backoff(
                    lambda: save_to_blob_storage(buffer.read(), model_blob_name),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )()
                logging.info(f"Trained model saved to blob: {model_blob_name}")
            except Exception as e:
                logging.error(f"Failed to save trained model to blob: {e}", exc_info=True)

            # --- Logging for Debugging ---
            logging.info(f"Model coefficients: {coefs}")
            logging.info(f"Model intercept: {intercept}")
            logging.info(f"Predictions: {y_pred.tolist()}")
            logging.info(f"Actuals: {y_test.tolist()}")

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
            return json_response(result, 200)
        elif req.method == "GET":
            # Retrieve most recent regression results (implement as needed)
            return error_response("GET /regression not implemented yet.", 501)
        else:
            return error_response("Method Not Allowed", 405)
    except Exception as e:
        logging.error(f"Unexpected error in regression endpoint: {e}", exc_info=True)
        return error_response("An unexpected error occurred in regression.", 500)
