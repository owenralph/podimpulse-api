from typing import List, Dict
import logging
import pandas as pd
import feedparser
import pytz
from dateutil import parser
from utils.constants import TIMEZONE


def parse_rss_feed(rss_url: str) -> pd.DataFrame:
    """Fetches and parses the RSS feed, returning episode titles and publication dates."""
    try:
        logging.info(f"Fetching and parsing RSS feed from: {rss_url}")
        feed = feedparser.parse(rss_url)
        london_tz = pytz.timezone(TIMEZONE)
        utc_tz = pytz.utc
        episode_data = []

        for entry in feed.entries:
            if hasattr(entry, 'published') and hasattr(entry, 'title'):
                published_date = parser.parse(entry.published)
                if published_date.tzinfo is None:
                    published_date = utc_tz.localize(published_date)
                localized_date = published_date.astimezone(london_tz).date()
                title = entry.title
                episode_data.append({"Date": localized_date, "Title": title})

        if not episode_data:
            logging.warning("No valid episodes found in the RSS feed.")

        # Convert to DataFrame
        return pd.DataFrame(episode_data)
    except Exception as e:
        raise ValueError(f"Error parsing RSS feed: {e}")
