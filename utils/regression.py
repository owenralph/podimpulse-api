import pandas as pd
import numpy as np
from utils.retry import retry_with_backoff
from utils.azure_blob import blob_container_client
from utils import handle_errors, require_columns
import logging
from typing import List, Tuple

def load_json_from_blob(token: str) -> str:
    """
    Loads a JSON string from blob storage using the given token as the blob name (with .json extension).
    Retries on failure.
    """
    def load_blob():
        blob_name = f"{token}.json"
        blob_client = blob_container_client.get_blob_client(blob_name)
        return blob_client.download_blob().readall().decode('utf-8')
    return retry_with_backoff(
        load_blob,
        exceptions=(Exception,),
        max_attempts=3,
        initial_delay=1.0,
        backoff_factor=2.0
    )

@handle_errors
def add_lagged_episode_release_columns(df: pd.DataFrame, max_days: int = 7) -> pd.DataFrame:
    """
    Adds columns for episodes released in the past 0 to max_days days.

    Args:
        df (pd.DataFrame): DataFrame with an 'Episodes Released' column.
        max_days (int): Maximum lag (number of days) to create columns for.

    Returns:
        pd.DataFrame: DataFrame with new lagged columns added.
    """
    logging.debug(f"Adding lagged episode release columns up to {max_days} days.")
    require_columns(df, ['Episodes Released'])
    # Add lagged columns for each day up to max_days
    for i in range(max_days + 1):
        col_name = f"Episodes released today-{i}"
        df[col_name] = df['Episodes Released'].shift(i).fillna(0)
    return df

@handle_errors
def summarize_impact_results(results: List[dict]) -> Tuple[int, float, List[dict]]:
    """
    Summarizes regression results for episode impact analysis.

    Args:
        results (List[dict]): List of result dictionaries with 'impact' and 'day_offset'.

    Returns:
        Tuple[int, float, List[dict]]: (days_of_impact, average_impact, impact_per_day)
            - days_of_impact (int): Number of days with impact results.
            - average_impact (float): Mean impact value.
            - impact_per_day (List[dict]): List of impact per day with 'day_offset' and 'impact'.
    """
    logging.debug(f"Summarizing impact results for {len(results) if results else 0} entries.")
    if results:
        days_of_impact = len(results)
        average_impact = float(np.mean([result['impact'] for result in results]))
        impact_per_day = [
            {
                'day_offset': int(result['day_offset']),
                'impact': float(result['impact'])
            } for result in results
        ]
    else:
        days_of_impact = 0
        average_impact = 0.0
        impact_per_day = []
    return days_of_impact, average_impact, impact_per_day