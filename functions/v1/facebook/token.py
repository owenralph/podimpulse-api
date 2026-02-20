import azure.functions as func
from utils.constants import APP_ID, APP_SECRET
import requests
import logging
from utils import validate_http_method, json_response, error_response


def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to exchange a Facebook user token for a page token.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the exchanged token or error message.
    """
    logging.debug("[exchange_user_token] Received request to exchange Facebook user token.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    if not APP_ID or not APP_SECRET:
        return error_response("Facebook app credentials are not configured.", 500)

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        user_token = body.get("user_token")
        if not user_token:
            return error_response("Missing 'user_token' parameter.", 400)

        # Exchange the short-lived token for a long-lived token
        url = f"https://graph.facebook.com/v17.0/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": APP_ID,
            "client_secret": APP_SECRET,
            "fb_exchange_token": user_token,
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        long_lived_token = response.json().get("access_token")
        if not long_lived_token:
            return error_response("Failed to exchange token.", 500)

        return json_response({"long_lived_user_token": long_lived_token}, 200)
    except requests.RequestException as e:
        logging.error(f"Facebook API error exchanging user token: {e}", exc_info=True)
        return error_response("Facebook API request failed.", 502)
    except Exception as e:
        logging.error(f"Error exchanging user token: {e}", exc_info=True)
        return error_response("Error exchanging user token.", 500)


def get_page_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get a Facebook page token.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the page token or error message.
    """
    logging.debug("[get_page_token] Received request to get Facebook page token.")
    method_error = validate_http_method(req, ["POST"])
    if method_error:
        return method_error

    try:
        body = req.get_json()
    except ValueError:
        return error_response("Invalid JSON body.", 400)

    try:
        user_token = body.get("user_token")
        page_id = body.get("page_id")
        if not user_token or not page_id:
            return error_response("Missing 'user_token' or 'page_id' parameter.", 400)

        # Fetch the page token
        url = f"https://graph.facebook.com/v17.0/{page_id}"
        params = {"access_token": user_token, "fields": "access_token"}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        page_token = response.json().get("access_token")
        if not page_token:
            return error_response("Failed to fetch page token.", 500)

        return json_response({"page_id": page_id, "page_token": page_token}, 200)
    except requests.RequestException as e:
        logging.error(f"Facebook API error fetching page token: {e}", exc_info=True)
        return error_response("Facebook API request failed.", 502)
    except Exception as e:
        logging.error(f"Error fetching page token: {e}", exc_info=True)
        return error_response("Error fetching page token.", 500)
