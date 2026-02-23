import logging
import time

import azure.functions as func
import requests

from utils import error_response, json_response, validate_http_method
from utils.retry import retry_with_backoff


def query_video_analytics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Query TikTok video analytics for the authenticated account.
    """
    logging.debug("[query_video_analytics] Received request to query TikTok analytics.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        account_token = body.get("account_token")
        max_count = body.get("max_count", 20)
        if not account_token:
            return error_response("Missing 'account_token' parameter.", 400)

        try:
            max_count = int(max_count)
        except (TypeError, ValueError):
            return error_response("Invalid 'max_count' parameter. Must be an integer.", 400)
        max_count = max(1, min(100, max_count))

        url = "https://open.tiktokapis.com/v2/video/list/"
        params = {
            "fields": (
                "id,title,video_description,create_time,share_count,"
                "view_count,like_count,comment_count"
            )
        }
        headers = {
            "Authorization": f"Bearer {account_token}",
            "Content-Type": "application/json",
        }
        payload = {"max_count": max_count}

        def fetch_video_analytics():
            call_start = time.perf_counter()
            response = requests.post(
                url, params=params, json=payload, headers=headers, timeout=10
            )
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=tiktok.query_account_analytics "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            fetch_video_analytics,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="tiktok.query_account_analytics",
        )()

        videos = response.json().get("data", {}).get("videos", [])
        processed_videos = []

        for video in videos:
            processed_videos.append(
                {
                    "id": video.get("id"),
                    "create_time": video.get("create_time"),
                    "title": video.get("title"),
                    "description": video.get("video_description", ""),
                    "insights": {
                        "view_count": video.get("view_count"),
                        "like_count": video.get("like_count"),
                        "comment_count": video.get("comment_count"),
                        "share_count": video.get("share_count"),
                    },
                }
            )

        return json_response({"status": "success", "videos": processed_videos}, 200)
    except requests.RequestException as e:
        logging.error(f"TikTok API error querying account analytics: {e}", exc_info=True)
        return error_response("TikTok API request failed.", 502)
    except Exception as e:
        logging.error(f"Error querying TikTok analytics: {e}", exc_info=True)
        return error_response("Error querying account analytics.", 500)
