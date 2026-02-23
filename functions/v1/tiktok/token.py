import logging
import time

import azure.functions as func
import requests

from utils import error_response, json_response, validate_http_method
from utils.constants import TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET
from utils.retry import retry_with_backoff


def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Exchange a TikTok OAuth authorization code for an access token.
    """
    logging.debug("[exchange_user_token] Received request to exchange TikTok user token.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    if not TIKTOK_CLIENT_KEY or not TIKTOK_CLIENT_SECRET:
        return error_response("TikTok app credentials are not configured.", 500)

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        auth_code = body.get("auth_code")
        redirect_uri = body.get("redirect_uri")
        if not auth_code or not redirect_uri:
            return error_response("Missing 'auth_code' or 'redirect_uri' parameter.", 400)

        url = "https://open.tiktokapis.com/v2/oauth/token/"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_key": TIKTOK_CLIENT_KEY,
            "client_secret": TIKTOK_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": redirect_uri,
        }

        def fetch_exchange():
            call_start = time.perf_counter()
            response = requests.post(url, data=data, headers=headers, timeout=10)
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=tiktok.exchange_user_token "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            fetch_exchange,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="tiktok.exchange_user_token",
        )()

        token_data = response.json()
        payload = token_data.get("data", token_data)
        access_token = payload.get("access_token")
        if not access_token:
            return error_response("Failed to exchange token.", 500)

        return json_response(
            {
                "access_token": access_token,
                "refresh_token": payload.get("refresh_token"),
                "open_id": payload.get("open_id"),
                "scope": payload.get("scope"),
                "expires_in": payload.get("expires_in"),
                "refresh_expires_in": payload.get("refresh_expires_in"),
            },
            200,
        )
    except requests.RequestException as e:
        logging.error(f"TikTok API error exchanging user token: {e}", exc_info=True)
        return error_response("TikTok API request failed.", 502)
    except Exception as e:
        logging.error(f"Error exchanging TikTok user token: {e}", exc_info=True)
        return error_response("Error exchanging user token.", 500)


def get_account_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Validate a TikTok user token against an account ID and return an account token.
    """
    logging.debug("[get_account_token] Received request to get TikTok account token.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        user_token = body.get("user_token")
        account_id = body.get("account_id")
        if not user_token or not account_id:
            return error_response("Missing 'user_token' or 'account_id' parameter.", 400)

        url = "https://open.tiktokapis.com/v2/user/info/"
        params = {"fields": "open_id,display_name"}
        headers = {"Authorization": f"Bearer {user_token}"}

        def fetch_account_info():
            call_start = time.perf_counter()
            response = requests.get(url, params=params, headers=headers, timeout=10)
            elapsed_ms = (time.perf_counter() - call_start) * 1000
            logging.info(
                f"[metric] external_http.call operation=tiktok.get_account_token "
                f"status={response.status_code} duration_ms={elapsed_ms:.2f} timeout_s=10"
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            fetch_account_info,
            exceptions=(requests.RequestException,),
            max_attempts=3,
            initial_delay=1.0,
            backoff_factor=2.0,
            operation_name="tiktok.get_account_token",
        )()

        payload = response.json().get("data", {}).get("user", {})
        open_id = payload.get("open_id")
        if not open_id:
            return error_response("Failed to fetch account details.", 500)
        if open_id != account_id:
            return error_response("Provided 'account_id' does not match authenticated user.", 403)

        return json_response({"account_id": account_id, "account_token": user_token}, 200)
    except requests.RequestException as e:
        logging.error(f"TikTok API error fetching account token: {e}", exc_info=True)
        return error_response("TikTok API request failed.", 502)
    except Exception as e:
        logging.error(f"Error fetching TikTok account token: {e}", exc_info=True)
        return error_response("Error fetching account token.", 500)
