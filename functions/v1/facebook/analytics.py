import azure.functions as func
import json
import requests
import logging


def query_reels_analytics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to query Facebook Reels analytics for a page.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with analytics data or error message.
    """
    logging.debug("[query_reels_analytics] Received request to query Facebook Reels analytics.")
    try:
        body = req.get_json()
        page_token = body.get("page_token")
        if not page_token:
            return func.HttpResponse(
                json.dumps({"error": "Missing 'page_token' parameter."}),
                mimetype="application/json",
                status_code=400
            )

        # Define endpoint and parameters
        reels_url = "https://graph.facebook.com/v20.0/me/video_reels"
        reels_params = {
            "access_token": page_token,
            "fields": "views,description,updated_time,video_insights",
            "limit": 100  # Adjust as needed
        }

        # Fetch Reels data
        reels_response = requests.get(reels_url, params=reels_params)
        reels_response.raise_for_status()

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

        return func.HttpResponse(
            json.dumps({"status": "success", "reels": processed_reels}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error querying Reels analytics: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )