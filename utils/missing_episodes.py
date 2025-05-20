import pandas as pd
from typing import Any


def mark_potential_missing_episodes(
    downloads_df: pd.DataFrame,
    episode_dates: pd.Series
) -> pd.DataFrame:
    """
    Marks spikes as potential missing episodes if they don't match any episode release dates
    and there are no episodes released on that date. Also adds a deduced episodes released predictor.

    Args:
        downloads_df (pd.DataFrame): DataFrame containing download data with 'is_spike', 'is_anomalous',
                                     and 'Episodes Released' columns.
        episode_dates (pd.Series): Series of episode release dates.

    Returns:
        pd.DataFrame: Updated DataFrame with 'potential_missing_episode' and 'deduced_episodes_released' columns.

    Raises:
        ValueError: If required columns are missing.
    """
    if 'is_spike' not in downloads_df.columns or 'is_anomalous' not in downloads_df.columns or 'Episodes Released' not in downloads_df.columns:
        raise ValueError("The dataset must have 'is_spike', 'is_anomalous', and 'Episodes Released' columns.")

    # Normalize episode release dates to midnight for comparison
    episode_dates = pd.to_datetime(episode_dates).dt.normalize()

    # Create a column for potential missing episodes
    downloads_df['potential_missing_episode'] = downloads_df.apply(
        lambda row: (
            row['is_spike'] and not row['is_anomalous'] and
            row['Episodes Released'] == 0 and
            (row['Date'].normalize() not in episode_dates)
        ),
        axis=1
    )

    # Calculate deduced episodes released
    downloads_df['deduced_episodes_released'] = downloads_df.apply(
        lambda row: row['Episodes Released'] + (1 if row['potential_missing_episode'] else 0),
        axis=1
    )

    return downloads_df
