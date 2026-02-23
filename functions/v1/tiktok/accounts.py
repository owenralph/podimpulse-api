import logging
import time

import azure.functions as func
import requests

from utils import error_response, json_response, validate_http_method
from utils.retry import retry_with_backoff


def get_user_accounts(req: func.HttpRequest) -> func.HttpResponse:
    """
    Return the authenticated TikTok account as a single-item accounts list.
    """
    logging.debug("[get_user_accounts] Received request to get TikTok user accounts.")
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

        url = "https://open.tiktokapis.com/v2/user/info/"
        params = {"fields": "open_id,display_name"}
        headers = {"Authorization": f"Bearer {user_token}"}

        def fetch_user_account():
            call_start = time.perf_counter()
            response = requests.get(url, params=params, headers=headers, timeout=10)
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=tiktok.get_user_accounts "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            fetch_user_account,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="tiktok.get_user_accounts",
        )()

        account = response.json().get("data", {}).get("user", {})
        account_id = account.get("open_id")
        account_name = account.get("display_name") or "TikTok Account"

        accounts = []
        if account_id:
            accounts.append({"id": account_id, "name": account_name})

        return json_response({"accounts": accounts}, 200)
    except requests.RequestException as e:
        logging.error(f"TikTok API error fetching user accounts: {e}", exc_info=True)
        return error_response("TikTok API request failed.", 502)
    except Exception as e:
        logging.error(f"Error fetching TikTok user accounts: {e}", exc_info=True)
        return error_response("Error fetching user accounts.", 500)
