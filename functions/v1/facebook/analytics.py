import azure.functions as func
import requests
import logging
from utils import validate_http_method, json_response, error_response
from utils.retry import retry_with_backoff
import time


def query_reels_analytics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to query Facebook Reels analytics for a page.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with analytics data or error message.
    """
    logging.debug("[query_reels_analytics] Received request to query Facebook Reels analytics.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        page_token = body.get("page_token")
        if not page_token:
            return error_response("Missing 'page_token' parameter.", 400)

        # Define endpoint and parameters
        reels_url = "https://graph.facebook.com/v20.0/me/video_reels"
        reels_params = {
            "access_token": page_token,
            "fields": "views,description,updated_time,video_insights",
            "limit": 100  # Adjust as needed
        }

        # Fetch Reels data
        def fetch_reels_data():
            call_start = time.perf_counter()
            response = requests.get(reels_url, params=reels_params, timeout=10)
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=facebook.query_page_analytics "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        reels_response = retry_with_backoff(
            fetch_reels_data,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="facebook.query_page_analytics",
        )()

        # Parse Reels data
        reels_data = reels_response.json().get("data", [])
        processed_reels = []

        for reel in reels_data:
            reel_details = {
                "id": reel.get("id"),
                "views": reel.get("views"),
                "updated_time": reel.get("updated_time"),
                "description": reel.get("description", ""),
                "insights": {}
            }

            # Parse insights
            video_insights = reel.get("video_insights", {}).get("data", [])
            for insight in video_insights:
                name = insight.get("name")
                value = insight.get("values", [{}])[0].get("value", {})
                reel_details["insights"][name] = value

            processed_reels.append(reel_details)

        return json_response({"status": "success", "reels": processed_reels}, 200)
    except requests.RequestException as e:
        logging.error(f"Facebook API error querying reels analytics: {e}", exc_info=True)
        return error_response("Facebook API request failed.", 502)
    except Exception as e:
        logging.error(f"Error querying Reels analytics: {e}", exc_info=True)
        return error_response("Error querying reels analytics.", 500)
