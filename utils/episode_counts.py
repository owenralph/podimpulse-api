import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from kneed import KneeLocator
import pytz
from utils import handle_errors, require_columns
import logging


@handle_errors
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
    logging.debug(f"Adding episode counts and clustering titles with max_clusters={max_clusters}.")
    require_columns(downloads_df, ['Date'])
    require_columns(episode_data, ['Date', 'Title'])

    # Ensure both DataFrames have the 'Date' column as datetime64[ns] with UTC
    # Use dayfirst=True and errors='coerce' to robustly parse mixed date formats
    downloads_df['Date'] = pd.to_datetime(downloads_df['Date'], utc=True, dayfirst=True, errors='coerce')
    episode_data['Date'] = pd.to_datetime(episode_data['Date'], utc=True, dayfirst=True, errors='coerce')
    # Drop rows where date parsing failed
    downloads_df = downloads_df.dropna(subset=['Date'])
    episode_data = episode_data.dropna(subset=['Date'])

    # Convert RSS episode dates to UK local time (Europe/London) and extract local date
    uk_tz = pytz.timezone('Europe/London')
    episode_data['Local_Date'] = episode_data['Date'].dt.tz_convert(uk_tz).dt.date
    downloads_df['Local_Date'] = downloads_df['Date'].dt.tz_convert(uk_tz).dt.date

    # Group episode titles by local date
    episode_titles_grouped = episode_data.groupby("Local_Date")["Title"].apply(list).reset_index()
    episode_titles_grouped.rename(columns={"Title": "Episode_Titles"}, inplace=True)
    episode_titles_grouped['Episodes Released'] = episode_titles_grouped['Episode_Titles'].apply(len)

    # Merge episode counts/titles into downloads_df on Local_Date
    downloads_df = downloads_df.merge(episode_titles_grouped, on='Local_Date', how='left')
    downloads_df['Episodes Released'] = downloads_df['Episodes Released'].fillna(0).astype(int)
    downloads_df['Episode_Titles'] = downloads_df['Episode_Titles'].apply(lambda x: x if isinstance(x, list) else [])

    # Flatten titles for clustering
    flattened_titles = episode_data["Title"].dropna().unique()
    if len(flattened_titles) == 0:
        downloads_df["Clustered_Episode_Titles"] = downloads_df["Episode_Titles"].apply(
            lambda titles: [{"title": title, "cluster": -1} for title in titles]
        )
        downloads_df = downloads_df.drop(columns=["Local_Date"])
        return downloads_df

    max_k = min(max_clusters, len(flattened_titles))
    if max_k < 1:
        max_k = 1

    # Vectorize episode titles using TF-IDF
    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf_matrix = vectorizer.fit_transform(flattened_titles)

    # Determine optimal number of clusters using the elbow method
    ssd = []  # Sum of squared distances
    for k in range(1, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(tfidf_matrix)
        ssd.append(kmeans.inertia_)

    knee = KneeLocator(range(1, max_k + 1), ssd, curve="convex", direction="decreasing")
    optimal_clusters = knee.knee or min(2, max_k)

    # Apply KMeans clustering
    kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
    title_clusters = kmeans.fit_predict(tfidf_matrix)

    # Map clusters back to episode titles
    title_cluster_map = {title: cluster for title, cluster in zip(flattened_titles, title_clusters)}

    # Add clusters to episode titles
    downloads_df["Clustered_Episode_Titles"] = downloads_df["Episode_Titles"].apply(
        lambda titles: [{"title": title, "cluster": title_cluster_map.get(title, -1)} for title in titles]
    )

    # Remove Local_Date column before returning (optional, for cleanliness)
    downloads_df = downloads_df.drop(columns=['Local_Date'])

    return downloads_df
