import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from kneed import KneeLocator
import logging
from typing import Any
from utils import handle_errors, require_columns


@handle_errors
def determine_optimal_clusters(features: np.ndarray, max_clusters: int = 10) -> int:
    """
    Uses the elbow method to determine the optimal number of clusters.

    Args:
        features (np.ndarray): Scaled feature array for clustering.
        max_clusters (int): Maximum number of clusters to consider.

    Returns:
        int: Optimal number of clusters based on the elbow method.
    """
    logging.debug(f"Determining optimal clusters up to {max_clusters}.")
    ssd = []  # Sum of squared distances for each k
    for k in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(features)
        ssd.append(kmeans.inertia_)  # Inertia is the sum of squared distances
    knee = KneeLocator(range(1, max_clusters + 1), ssd, curve="convex", direction="decreasing")
    optimal_clusters = knee.knee or 2  # Default to 2 clusters if no knee is found
    return optimal_clusters


@handle_errors
def characterize_clusters(spike_data: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes and characterizes clusters to identify anomalies or special cases.

    Args:
        spike_data (pd.DataFrame): DataFrame containing spikes with cluster labels.

    Returns:
        pd.DataFrame: Updated DataFrame with cluster characterization columns.
    """
    logging.debug("Characterizing clusters in spike data.")
    require_columns(spike_data, ['spike_cluster', 'spike_height', 'tail_decay', 'spike_timing'])
    cluster_stats = spike_data.groupby('spike_cluster').agg({
        'spike_height': ['mean', 'std'],
        'tail_decay': ['mean', 'std'],
        'spike_timing': ['mean', 'std', 'count']
    }).reset_index()
    cluster_stats.columns = [
        'spike_cluster', 
        'height_mean', 'height_std', 
        'decay_mean', 'decay_std', 
        'timing_mean', 'timing_std', 'cluster_size'
    ]
    spike_data = spike_data.merge(cluster_stats, on='spike_cluster', how='left')
    spike_data['is_anomalous'] = (
        (spike_data['spike_height'] > spike_data['height_mean'] + 2 * spike_data['height_std']) |
        (spike_data['spike_height'] < spike_data['height_mean'] - 2 * spike_data['height_std']) |
        (spike_data['tail_decay'] > spike_data['decay_mean'] + 2 * spike_data['decay_std']) |
        (spike_data['cluster_size'] < 3)  # Small cluster size could indicate an anomaly
    )
    return spike_data


@handle_errors
def perform_spike_clustering(downloads_df: pd.DataFrame, max_clusters: int = 10) -> pd.DataFrame:
    """
    Performs spike detection and clustering on the downloads DataFrame.

    Args:
        downloads_df (pd.DataFrame): DataFrame with download data.
        max_clusters (int): Maximum number of clusters to consider.

    Returns:
        pd.DataFrame: DataFrame with spike and cluster information.
    """
    logging.debug(f"Performing spike clustering with max_clusters={max_clusters}.")
    require_columns(downloads_df, ['Date', 'Downloads'])
    downloads_df['Date'] = pd.to_datetime(downloads_df['Date'])
    downloads_df['rolling_mean'] = downloads_df['Downloads'].rolling(window=7, min_periods=1).mean()
    downloads_df['rolling_std'] = downloads_df['Downloads'].rolling(window=7, min_periods=1).std()
    stability_threshold = 7
    stable_data = downloads_df.iloc[stability_threshold:].copy()
    stable_data['z_score'] = (stable_data['Downloads'] - stable_data['rolling_mean']) / stable_data['rolling_std']
    stable_data['is_spike'] = stable_data['z_score'] > 2
    stable_data['is_anomalous'] = stable_data['z_score'] > 3
    spike_data = stable_data[stable_data['is_spike']].copy()
    if spike_data.empty:
        logging.warning("No spikes detected. Returning original dataset.")
        downloads_df['is_anomalous'] = False
        downloads_df['is_spike'] = False
        return downloads_df
    spike_data['spike_height'] = spike_data['Downloads'] - spike_data['rolling_mean']
    spike_data['spike_timing'] = (spike_data['Date'] - spike_data['Date'].min()).dt.days
    spike_data['tail_decay'] = spike_data['Downloads'] - spike_data['Downloads'].shift(1).fillna(0)
    features = spike_data[['spike_height', 'spike_timing', 'tail_decay']].fillna(0)
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    optimal_clusters = determine_optimal_clusters(scaled_features, max_clusters)
    kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
    spike_data['spike_cluster'] = kmeans.fit_predict(scaled_features)
    cluster_dummies = pd.get_dummies(spike_data['spike_cluster'], prefix='spike_cluster')
    spike_data = pd.concat([spike_data[['Date', 'is_anomalous', 'is_spike']], cluster_dummies], axis=1)
    downloads_df = downloads_df.merge(
        spike_data,
        on='Date',
        how='left'
    )
    downloads_df['is_spike'] = downloads_df['is_spike'].fillna(False)
    downloads_df['is_anomalous'] = downloads_df['is_anomalous'].fillna(False)
    for col in downloads_df.columns:
        if col.startswith('spike_cluster_'):
            downloads_df[col] = downloads_df[col].fillna(0).astype(int)
    return downloads_df
