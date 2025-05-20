import pandas as pd
import numpy as np
from utils.retry import retry_with_backoff
from utils.azure_blob import blob_container_client

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

def add_lagged_episode_release_columns(df: pd.DataFrame, max_days: int = 7) -> pd.DataFrame:
    """
    Adds columns for episodes released in the past 0 to max_days days.
    """
    for i in range(max_days + 1):
        col_name = f"Episodes released today-{i}"
        df[col_name] = df['Episodes Released'].shift(i).fillna(0)
    return df

def summarize_impact_results(results):
    """
    Summarizes regression results for episode impact analysis.
    Returns days_of_impact, average_impact, and impact_per_day list.
    """
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