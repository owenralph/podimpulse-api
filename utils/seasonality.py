import pandas as pd
import numpy as np
import logging
from typing import Any
import pytz

def add_seasonality_predictors(df: pd.DataFrame, date_col: str = 'Date', release_time_col: str = None) -> pd.DataFrame:
    """
    Adds seasonality predictors (day_of_week, month, and their cyclical encodings) to the DataFrame.
    Optionally adds hour_sin and hour_cos if a release_time_col is provided.
    Args:
        df (pd.DataFrame): The input DataFrame containing a date column.
        date_col (str): The name of the date column to use for extracting seasonality features.
        release_time_col (str, optional): The name of the column with release time (datetime or ISO string).
    Returns:
        pd.DataFrame: DataFrame with added seasonality predictor columns.
    Raises:
        ValueError: If the date column is missing or cannot be parsed.
    """
    if date_col not in df.columns:
        raise ValueError(f"Column '{date_col}' not found in DataFrame.")
    try:
        df['day_of_week'] = df[date_col].apply(lambda d: pd.to_datetime(d).weekday())
        df['month'] = df[date_col].apply(lambda d: pd.to_datetime(d).month)
        df['day_of_week_sin'] = df['day_of_week'].apply(lambda x: np.sin(2 * np.pi * x / 7))
        df['day_of_week_cos'] = df['day_of_week'].apply(lambda x: np.cos(2 * np.pi * x / 7))
        df['month_sin'] = df['month'].apply(lambda x: np.sin(2 * np.pi * x / 12))
        df['month_cos'] = df['month'].apply(lambda x: np.cos(2 * np.pi * x / 12))
        # Optionally add hour_sin and hour_cos
        if release_time_col and release_time_col in df.columns:
            def extract_hour(val):
                try:
                    if pd.isnull(val):
                        return 0.0
                    dt = pd.to_datetime(val, utc=True)
                    # Convert to London local time (handles DST)
                    dt_london = dt.tz_convert('Europe/London')
                    return dt_london.hour + dt_london.minute / 60.0
                except Exception as ex:
                    logging.warning(f"Failed to convert release time '{val}' to London local hour: {ex}")
                    return 0.0
            df['release_hour'] = df[release_time_col].apply(extract_hour)
            df['hour_sin'] = df['release_hour'].apply(lambda h: np.sin(2 * np.pi * h / 24) if h != 0 else 0.0)
            df['hour_cos'] = df['release_hour'].apply(lambda h: np.cos(2 * np.pi * h / 24) if h != 0 else 0.0)
    except Exception as e:
        logging.error(f"Failed to add seasonality predictors: {e}", exc_info=True)
        raise
    return df
