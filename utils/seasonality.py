import pandas as pd
import numpy as np
import logging
from typing import Any
from utils import handle_errors, require_columns

@handle_errors
def add_seasonality_predictors(df: pd.DataFrame, date_col: str = 'Date') -> pd.DataFrame:
    """
    Adds seasonality predictors (day_of_week, month, and their cyclical encodings) to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing a date column.
        date_col (str): The name of the date column to use for extracting seasonality features.

    Returns:
        pd.DataFrame: DataFrame with added seasonality predictor columns.

    Raises:
        ValueError: If the date column is missing or cannot be parsed.
    """
    logging.debug(f"Adding seasonality predictors using column '{date_col}'.")
    require_columns(df, [date_col])
    try:
        df['day_of_week'] = df[date_col].apply(lambda d: pd.to_datetime(d).weekday())
        df['month'] = df[date_col].apply(lambda d: pd.to_datetime(d).month)
        df['day_of_week_sin'] = df['day_of_week'].apply(lambda x: np.sin(2 * np.pi * x / 7))
        df['day_of_week_cos'] = df['day_of_week'].apply(lambda x: np.cos(2 * np.pi * x / 7))
        df['month_sin'] = df['month'].apply(lambda x: np.sin(2 * np.pi * x / 12))
        df['month_cos'] = df['month'].apply(lambda x: np.cos(2 * np.pi * x / 12))
    except Exception as e:
        logging.error(f"Failed to add seasonality predictors: {e}", exc_info=True)
        raise
    return df
