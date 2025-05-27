import azure.functions as func

def rss(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to get or update the RSS feed URL for a podcast.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response with the RSS feed URL or error message.
    """
    # Logic for RSS feed handling has been merged into the podcast resource handler (/v1/podcasts/{podcast_id}) and this file is now deprecated.
    return func.HttpResponse(
        "RSS feed handling has been merged into the podcast resource handler.",
        status_code=410
    )
