from typing import List
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from kneed import KneeLocator
import numpy as np


def add_episode_counts_and_titles(
    downloads_df: pd.DataFrame,
    episode_data: pd.DataFrame,
    max_clusters: int = 10
) -> pd.DataFrame:
    """
    Adds episode release counts, titles, and clusters to the download DataFrame.

    Args:
        downloads_df (pd.DataFrame): Download data with dates.
        episode_data (pd.DataFrame): Episode data with dates and titles.
        max_clusters (int): Maximum number of clusters to consider for the elbow method.

    Returns:
        pd.DataFrame: Updated DataFrame with episode counts, titles, and cluster information.
    """
    # Ensure both DataFrames have the 'Date' column as datetime64[ns]
    downloads_df['Date'] = pd.to_datetime(downloads_df['Date'])
    episode_data['Date'] = pd.to_datetime(episode_data['Date'])

    # Group episode titles by date
    episode_titles_grouped = episode_data.groupby("Date")["Title"].apply(list).reset_index()
    episode_titles_grouped.rename(columns={"Title": "Episode_Titles"}, inplace=True)

    # Add episode counts to the grouped DataFrame
    episode_titles_grouped['Episodes Released'] = episode_titles_grouped['Episode_Titles'].apply(len)

    # Flatten titles for clustering
    flattened_titles = episode_data["Title"].dropna().unique()

    # Vectorize episode titles using TF-IDF
    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf_matrix = vectorizer.fit_transform(flattened_titles)

    # Determine optimal number of clusters using the elbow method
    ssd = []  # Sum of squared distances
    for k in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(tfidf_matrix)
        ssd.append(kmeans.inertia_)

    knee = KneeLocator(range(1, max_clusters + 1), ssd, curve="convex", direction="decreasing")
    optimal_clusters = knee.knee or 2  # Default to 2 clusters if no elbow is found

    # Apply KMeans clustering
    kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
    title_clusters = kmeans.fit_predict(tfidf_matrix)

    # Map clusters back to episode titles
    title_cluster_map = {title: cluster for title, cluster in zip(flattened_titles, title_clusters)}

    # Add clusters to episode titles
    episode_titles_grouped["Clustered_Episode_Titles"] = episode_titles_grouped["Episode_Titles"].apply(
        lambda titles: [{"title": title, "cluster": title_cluster_map.get(title, -1)} for title in titles]
    )

    # Merge episode data with download data
    downloads_df = downloads_df.merge(episode_titles_grouped, on="Date", how="left")

    # Fill NaN with defaults for dates without episode releases
    downloads_df["Episode_Titles"] = downloads_df["Episode_Titles"].apply(lambda x: x if isinstance(x, list) else [])
    downloads_df["Clustered_Episode_Titles"] = downloads_df["Clustered_Episode_Titles"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    downloads_df["Episodes Released"] = downloads_df["Episodes Released"].fillna(0).astype(int)

    return downloads_df
