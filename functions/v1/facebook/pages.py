import azure.functions as func
import requests
import logging
from utils import validate_http_method, json_response, error_response
from utils.retry import retry_with_backoff
import time


def get_user_pages(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get Facebook user pages.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with user pages or error message.
    """
    logging.debug("[get_user_pages] Received request to get Facebook user pages.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        user_token = body.get("user_token")
        if not user_token:
            return error_response("Missing 'user_token' parameter.", 400)

        # Fetch user pages
        url = f"https://graph.facebook.com/v17.0/me/accounts"
        params = {"access_token": user_token}
        def fetch_user_pages():
            call_start = time.perf_counter()
            response = requests.get(url, params=params, timeout=10)
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=facebook.get_user_pages "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            fetch_user_pages,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="facebook.get_user_pages",
        )()

        pages = response.json().get("data", [])
        return json_response({"pages": [{"id": page["id"], "name": page["name"]} for page in pages]}, 200)
    except requests.RequestException as e:
        logging.error(f"Facebook API error fetching user pages: {e}", exc_info=True)
        return error_response("Facebook API request failed.", 502)
    except Exception as e:
        logging.error(f"Error fetching user pages: {e}", exc_info=True)
        return error_response("Error fetching user pages.", 500)
