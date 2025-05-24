import azure.functions as func
from utils.constants import APP_ID, APP_SECRET
import requests
import json
import logging


def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to exchange a Facebook user token for a page token.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the exchanged token or error message.
    """
    logging.debug("[exchange_user_token] Received request to exchange Facebook user token.")
    try:
        body = req.get_json()
        user_token = body.get("user_token")
        if not user_token:
            return func.HttpResponse("Missing 'user_token' parameter.", status_code=400)

        # Exchange the short-lived token for a long-lived token
        url = f"https://graph.facebook.com/v17.0/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": APP_ID,
            "client_secret": APP_SECRET,
            "fb_exchange_token": user_token,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()

        long_lived_token = response.json().get("access_token")
        if not long_lived_token:
            return func.HttpResponse("Failed to exchange token.", status_code=500)

        return func.HttpResponse(
            json.dumps({"long_lived_user_token": long_lived_token}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error exchanging user token: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


def get_page_token(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get a Facebook page token.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the page token or error message.
    """
    logging.debug("[get_page_token] Received request to get Facebook page token.")
    try:
        body = req.get_json()
        user_token = body.get("user_token")
        page_id = body.get("page_id")
        if not user_token or not page_id:
            return func.HttpResponse("Missing 'user_token' or 'page_id' parameter.", status_code=400)

        # Fetch the page token
        url = f"https://graph.facebook.com/v17.0/{page_id}"
        params = {"access_token": user_token, "fields": "access_token"}
        response = requests.get(url, params=params)
        response.raise_for_status()

        page_token = response.json().get("access_token")
        if not page_token:
            return func.HttpResponse("Failed to fetch page token.", status_code=500)

        return func.HttpResponse(
            json.dumps({"page_id": page_id, "page_token": page_token}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching page token: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
