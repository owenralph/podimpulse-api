import azure.functions as func
import requests
import logging
import json


def get_user_pages(req: func.HttpRequest) -> func.HttpResponse:
    """Fetches pages the user has access to using the long-lived user token."""
    try:
        body = req.get_json()
        user_token = body.get("user_token")
        if not user_token:
            return func.HttpResponse("Missing 'user_token' parameter.", status_code=400)

        # Fetch user pages
        url = f"https://graph.facebook.com/v17.0/me/accounts"
        params = {"access_token": user_token}
        response = requests.get(url, params=params)
        response.raise_for_status()

        pages = response.json().get("data", [])
        return func.HttpResponse(
            json.dumps({"pages": [{"id": page["id"], "name": page["name"]} for page in pages]}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error fetching user pages: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
