import logging
import json
import pandas as pd
from typing import Optional
from utils.constants import normalize_to_london_date, get_default_rss_day_properties
from utils.azure_blob import save_to_blob_storage, load_from_blob_storage
from utils.retry import retry_with_backoff
from utils.rss_parser import parse_rss_feed
from utils.seasonality import add_time_series_features
from utils.episode_counts import add_episode_counts_and_titles
import azure.functions as func

def rss(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get or update the RSS feed URL for an instance.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with the RSS URL or update status.
    """
    logging.info("Received request to handle RSS feed.")

    try:
        if req.method == "POST":
            try:
                request_data = req.get_json()
                instance_id: Optional[str] = request_data.get("instance_id")
                rss_url: Optional[str] = request_data.get("rss_url")

                if not instance_id or not rss_url:
                    logging.error("Missing instance_id or rss_url in request body.")
                    return func.HttpResponse(json.dumps({
                        "message": "Missing instance_id or rss_url.",
                        "result": None
                    }), status_code=400)

                # Save rss_url to blob storage (with retry)
                try:
                    # Try to load existing blob data, else start fresh
                    try:
                        blob_data = retry_with_backoff(
                            lambda: load_from_blob_storage(instance_id),
                            exceptions=(RuntimeError,),
                            max_attempts=3,
                            initial_delay=1.0,
                            backoff_factor=2.0
                        )()
                        json_data = json.loads(blob_data)
                    except Exception:
                        json_data = {}
                    json_data["rss_url"] = rss_url
                except Exception as e:
                    logging.error(f"Failed to load blob data: {e}", exc_info=True)
                    return func.HttpResponse(json.dumps({
                        "message": "Failed to load blob data.",
                        "result": None
                    }), status_code=500)

                # Parse RSS feed and build daily dataset
                try:
                    episode_df = parse_rss_feed(rss_url)
                except Exception as e:
                    logging.error(f"Failed to parse RSS feed: {e}", exc_info=True)
                    return func.HttpResponse(json.dumps({
                        "message": "Failed to parse RSS feed.",
                        "result": None
                    }), status_code=400)

                if episode_df.empty:
                    json_data["daily_dataset"] = []
                else:
                    # Normalize all episode dates to Europe/London local time (with DST)
                    episode_df['Date'] = normalize_to_london_date(episode_df['Date'])
                    min_date = episode_df['Date'].min()
                    max_date = episode_df['Date'].max()
                    all_days = pd.date_range(min_date, max_date, freq='D', tz=episode_df['Date'].dt.tz)
                    daily_df = pd.DataFrame({'Date': all_days})
                    # Ensure both DataFrames have 'Date' as datetime64[ns, Europe/London]
                    daily_df['Date'] = pd.to_datetime(daily_df['Date'])
                    episode_df['Date'] = pd.to_datetime(episode_df['Date'])
                    merged = daily_df.merge(episode_df, on='Date', how='left')
                    merged['Title'] = merged['Title'].apply(lambda x: [x] if pd.notnull(x) else [])
                    merged['episode_titles'] = merged['Title']
                    merged['episodes_released'] = merged['episode_titles'].apply(len)
                    merged['timezone'] = merged['Date'].dt.strftime('%Z')
                    try:
                        merged = add_episode_counts_and_titles(merged, episode_df)
                    except Exception as e:
                        logging.warning(f"Clustering failed or not enough data: {e}")
                        merged['Clustered_Episode_Titles'] = [[] for _ in range(len(merged))]

                    output = []
                    def py(v):
                        if hasattr(v, 'item'):
                            return v.item()
                        if isinstance(v, (list, tuple)):
                            return [py(x) for x in v]
                        if isinstance(v, dict):
                            return {k: py(val) for k, val in v.items()}
                        return v
                    for _, row in merged.iterrows():
                        output.append({
                            "date": row['Date'].strftime('%Y-%m-%d'),
                            "episode_titles": py(row['episode_titles']),
                            "episodes_released": py(row['episodes_released']),
                            "clustered_episode_titles": py(row.get('Clustered_Episode_Titles', [])),
                            "timezone": row['timezone']
                        })
                    json_data["daily_dataset"] = output

                # Save updated blob data with retry
                try:
                    retry_with_backoff(
                        lambda: save_to_blob_storage(json.dumps(json_data), instance_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )()
                except Exception as e:
                    logging.error(f"Failed to save blob data: {e}", exc_info=True)
                    return func.HttpResponse(json.dumps({
                        "message": "Failed to save blob data.",
                        "result": None
                    }), status_code=500)

                return func.HttpResponse(
                    json.dumps({
                        "message": "RSS feed URL and daily dataset updated successfully.",
                        "result": {
                            "instance_id": instance_id,
                            "rss_url": rss_url,
                            "daily_dataset_len": len(json_data.get("daily_dataset", [])),
                            "daily_dataset": json_data.get("daily_dataset", [])
                        }
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            except Exception as e:
                logging.error(f"Failed to update RSS feed: {e}", exc_info=True)
                return func.HttpResponse(
                    json.dumps({
                        "message": "Failed to update RSS feed.",
                        "result": None
                    }),
                    status_code=500
                )

        elif req.method == "GET":
            try:
                instance_id: Optional[str] = req.params.get("instance_id")
                if not instance_id:
                    logging.error("Missing instance_id in query parameters.")
                    return func.HttpResponse(json.dumps({
                        "message": "Missing instance_id.",
                        "result": None
                    }), status_code=400)
                try:
                    blob_data = retry_with_backoff(
                        lambda: load_from_blob_storage(instance_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )()
                    json_data = json.loads(blob_data)
                except Exception as e:
                    logging.error(f"Failed to load blob data: {e}", exc_info=True)
                    return func.HttpResponse(json.dumps({
                        "message": "Failed to load blob data.",
                        "result": None
                    }), status_code=500)
                if "rss_url" not in json_data:
                    logging.error("RSS feed URL not set.")
                    return func.HttpResponse(json.dumps({
                        "message": "RSS feed URL not set.",
                        "result": None
                    }), status_code=404)
                return func.HttpResponse(
                    json.dumps({
                        "message": "RSS feed URL retrieved successfully.",
                        "result": {"rss_url": json_data["rss_url"]}
                    }),
                    mimetype="application/json",
                    status_code=200
                )
            except Exception as e:
                logging.error(f"Failed to retrieve RSS feed: {e}", exc_info=True)
                return func.HttpResponse(
                    json.dumps({
                        "message": "Failed to retrieve RSS feed.",
                        "result": None
                    }),
                    status_code=500
                )

        else:
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(json.dumps({
                "message": "Method Not Allowed",
                "result": None
            }), status_code=405)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "message": "An unexpected error occurred.",
                "result": None
            }),
            status_code=500
        )
