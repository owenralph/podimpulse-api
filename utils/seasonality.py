import pandas as pd
import numpy as np
import logging
from typing import Any

def add_time_series_features(df, date_col='Date'):
    """
    Adds time series features (date-derived and Fourier) to the DataFrame.
    Features: day_of_week, month, week_of_year, is_weekend, is_month_start, is_month_end,
    is_quarter_start, is_quarter_end, day_of_week_sin/cos, month_sin/cos, fourier_sin/cos_k (k=1..4)
    """
    df = df.copy()
    dt = pd.to_datetime(df[date_col])
    df['day_of_week'] = dt.dt.dayofweek
    df['month'] = dt.dt.month
    df['week_of_year'] = dt.dt.isocalendar().week.astype(int)
    df['is_weekend'] = dt.dt.weekday >= 5
    df['is_month_start'] = dt.dt.is_month_start
    df['is_month_end'] = dt.dt.is_month_end
    df['is_quarter_start'] = dt.dt.is_quarter_start
    df['is_quarter_end'] = dt.dt.is_quarter_end
    # Seasonality (cyclical encoding)
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    df['month_sin'] = np.sin(2 * np.pi * (df['month'] - 1) / 12)
    df['month_cos'] = np.cos(2 * np.pi * (df['month'] - 1) / 12)
    # Fourier features for k=1..4 (annual seasonality)
    df['day_of_year'] = dt.dt.dayofyear
    for k in [1,2,3,4]:
        df[f'fourier_sin_{k}'] = np.sin(k * 2 * np.pi * df['day_of_year'] / 365)
        df[f'fourier_cos_{k}'] = np.cos(k * 2 * np.pi * df['day_of_year'] / 365)
    return df
