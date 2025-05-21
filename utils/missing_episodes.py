import pandas as pd
import logging
from typing import Any


def mark_potential_missing_episodes(
    downloads_df: pd.DataFrame,
    episode_dates: pd.Series,
    return_missing: bool = False
) -> Any:
    """
    Marks spikes as potential missing episodes if they don't match any episode release dates
    and there are no episodes released on that date. Also adds a deduced episodes released predictor.

    Args:
        downloads_df (pd.DataFrame): DataFrame containing download data with 'is_spike', 'is_anomalous',
                                     and 'Episodes Released' columns.
        episode_dates (pd.Series): Series of episode release dates.
        return_missing (bool): If True, also return a list of missing episode dates (ISO strings).

    Returns:
        pd.DataFrame or (pd.DataFrame, list):
            - If return_missing is False: Updated DataFrame with 'potential_missing_episode' and 'deduced_episodes_released' columns.
            - If return_missing is True: Tuple of (updated DataFrame, list of missing episode dates as ISO strings).

    Raises:
        ValueError: If required columns are missing.
    """
    if 'is_spike' not in downloads_df.columns or 'is_anomalous' not in downloads_df.columns or 'Episodes Released' not in downloads_df.columns:
        raise ValueError("The dataset must have 'is_spike', 'is_anomalous', and 'Episodes Released' columns.")

    # Normalize episode release dates to midnight for comparison
    episode_dates = pd.to_datetime(episode_dates).dt.normalize()
    logging.info(f"Normalized episode_dates: {episode_dates.tolist()}")

    # Detailed logging for each row
    def _potential_missing(row):
        date_norm = row['Date'].normalize()
        is_spike = row['is_spike']
        is_anomalous = row['is_anomalous']
        episodes_released = row['Episodes Released']
        not_in_episodes = date_norm not in episode_dates
        result = is_spike and not is_anomalous and episodes_released == 0 and not_in_episodes
        logging.info(
            f"Row date: {row['Date']} | Normalized: {date_norm} | is_spike: {is_spike} | "
            f"is_anomalous: {is_anomalous} | Episodes Released: {episodes_released} | "
            f"Not in episode_dates: {not_in_episodes} | Marked missing: {result}"
        )
        return result

    downloads_df['potential_missing_episode'] = downloads_df.apply(_potential_missing, axis=1)

    # Calculate deduced episodes released with logging
    def _deduced(row):
        deduced = row['Episodes Released'] + (1 if row['potential_missing_episode'] else 0)
        logging.info(
            f"Row date: {row['Date']} | Episodes Released: {row['Episodes Released']} | "
            f"Potential missing: {row['potential_missing_episode']} | Deduced: {deduced}"
        )
        return deduced

    downloads_df['deduced_episodes_released'] = downloads_df.apply(_deduced, axis=1)

    # Log all rows marked as potential missing
    missing_rows = downloads_df[downloads_df['potential_missing_episode']]
    logging.info(f"Rows marked as potential missing episodes: {missing_rows[['Date', 'is_spike', 'is_anomalous', 'Episodes Released']].to_dict('records')}")

    if return_missing:
        # Extract dates where potential_missing_episode is True
        missing_dates = downloads_df.loc[downloads_df['potential_missing_episode'], 'Date']
        # Convert to ISO strings for JSON serialization
        missing_dates_iso = pd.to_datetime(missing_dates).dt.strftime('%Y-%m-%d').tolist()
        logging.info(f"Missing episode dates (ISO): {missing_dates_iso}")
        return downloads_df, missing_dates_iso
    else:
        return downloads_df
