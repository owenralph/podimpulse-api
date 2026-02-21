import azure.functions as func
from utils import validate_http_method, json_response, handle_blob_operation, error_response
import logging
import json
import time
import uuid
from utils.retry import retry_with_backoff
from utils.azure_blob import (
    save_podcast_blob,
    load_podcast_blob,
    list_podcast_ids,
    delete_podcast_blob,
    get_podcast_id_from_index,
    create_podcast_index,
    delete_podcast_index,
    PodcastIndexConflictError,
)

def initialize(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to create or list podcast resources:
    - POST /v1/podcasts: create a new podcast
    - GET /v1/podcasts: list all podcasts
    """
    logging.debug("[initialize] Received request to create a new podcast.")
    start_time = time.time()

    # Validate HTTP method
    method_error = validate_http_method(req, ["POST", "GET"])
    if method_error:
        return method_error

    if req.method == "GET":
        try:
            existing_ids = list_podcast_ids(include_legacy=True)
            podcasts = []
            for pid in existing_ids:
                blob_data, err = handle_blob_operation(
                    retry_with_backoff(
                        lambda: load_podcast_blob(pid),
                        exceptions=(RuntimeError,),
                        max_attempts=2,
                        initial_delay=0.5,
                        backoff_factor=2.0
                    )
                )
                if err:
                    continue
                try:
                    pdata = json.loads(blob_data)
                except Exception:
                    continue
                if pdata.get("title") and pdata.get("rss_url"):
                    podcasts.append({
                        "podcast_id": pid,
                        "title": pdata["title"],
                        "rss_url": pdata["rss_url"]
                    })
            return json_response({
                "message": "Podcasts retrieved successfully.",
                "result": podcasts
            }, 200)
        except Exception as e:
            logging.error(f"Failed to list podcasts: {e}", exc_info=True)
            return error_response("Failed to list podcasts.", 500)

    # Parse and validate request body
    try:
        request_data = req.get_json()
    except Exception:
        return error_response("Invalid JSON body.", 400)
    title = request_data.get("title")
    rss_url = request_data.get("rss_url")
    if not title or not rss_url:
        return error_response("Missing or invalid title or rss_url.", 400)

    try:
        existing_by_title = get_podcast_id_from_index("title", title)
        existing_by_rss = get_podcast_id_from_index("rss", rss_url)
        if existing_by_title or existing_by_rss:
            return error_response("Podcast with this title or rss_url already exists.", 409)
    except Exception as e:
        logging.error(f"Failed to read podcast indexes: {e}", exc_info=True)
        return error_response("Failed to validate podcast uniqueness.", 500)

    podcast_id = str(uuid.uuid4())
    podcast_metadata = json.dumps({"title": title, "rss_url": rss_url})
    save_start = time.time()
    title_reserved = False
    rss_reserved = False

    try:
        retry_with_backoff(
            lambda: create_podcast_index("title", title, podcast_id, overwrite=False),
            exceptions=(RuntimeError,),
            max_attempts=3,
            initial_delay=0.2,
            backoff_factor=2.0,
        )()
        title_reserved = True

        retry_with_backoff(
            lambda: create_podcast_index("rss", rss_url, podcast_id, overwrite=False),
            exceptions=(RuntimeError,),
            max_attempts=3,
            initial_delay=0.2,
            backoff_factor=2.0,
        )()
        rss_reserved = True
    except PodcastIndexConflictError:
        if title_reserved:
            try:
                delete_podcast_index("title", title, expected_podcast_id=podcast_id)
            except Exception:
                pass
        return error_response("Podcast with this title or rss_url already exists.", 409)
    except Exception as e:
        logging.error(f"Failed to reserve podcast indexes: {e}", exc_info=True)
        return error_response("Failed to reserve podcast indexes.", 500)

    _, err = handle_blob_operation(
        retry_with_backoff(
            lambda: save_podcast_blob(podcast_metadata, podcast_id),
            exceptions=(RuntimeError,),
            max_attempts=3,
            initial_delay=0.5,
            backoff_factor=2.0,
        )
    )
    if err:
        if title_reserved:
            try:
                delete_podcast_index("title", title, expected_podcast_id=podcast_id)
            except Exception:
                pass
        if rss_reserved:
            try:
                delete_podcast_index("rss", rss_url, expected_podcast_id=podcast_id)
            except Exception:
                pass
        return error_response("Failed to create podcast.", 500)
    save_duration = time.time() - save_start
    logging.info(f"Podcast blob save completed in {save_duration:.2f} seconds.")

    # Return the podcast_id, title, and rss_url in a JSON response
    response_data = {
        "message": "Podcast created successfully.",
        "result": {"podcast_id": podcast_id, "title": title, "rss_url": rss_url}
    }
    total_duration = time.time() - start_time
    logging.info(f"Total function execution time: {total_duration:.2f} seconds.")
    return json_response(response_data, 201)

def podcast_resource(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handles GET, PUT, PATCH, DELETE for /v1/podcasts/{podcast_id}, including rss_url as a property.
    """
    method_error = validate_http_method(req, ["GET", "PUT", "PATCH", "DELETE"])
    if method_error:
        return method_error

    podcast_id = req.route_params.get("podcast_id")
    if not podcast_id:
        return error_response("Missing podcast_id in path.", 400)

    if req.method == "GET":
        try:
            blob_data, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_podcast_blob(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to load podcast data.", 404)
            json_data = json.loads(blob_data)
            if not json_data.get("title") or not json_data.get("rss_url"):
                return error_response("Podcast metadata incomplete.", 404)
            return json_response({
                "message": "Podcast retrieved successfully.",
                "result": {
                    "podcast_id": podcast_id,
                    "title": json_data["title"],
                    "rss_url": json_data["rss_url"]
                }
            }, 200)
        except Exception as e:
            logging.error(f"Failed to retrieve podcast: {e}", exc_info=True)
            return error_response("Failed to retrieve podcast.", 500)

    elif req.method in ("PUT", "PATCH"):
        try:
            request_data = req.get_json()
            title = request_data.get("title")
            rss_url = request_data.get("rss_url")
            if req.method == "PUT":
                if not title or not rss_url:
                    return error_response("Missing title or rss_url.", 400)
                old_blob_data, old_err = handle_blob_operation(
                    retry_with_backoff(
                        lambda: load_podcast_blob(podcast_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0,
                    )
                )
                if old_err:
                    return error_response("Failed to load podcast data.", 404)
                old_json_data = json.loads(old_blob_data)
                old_title = old_json_data.get("title")
                old_rss_url = old_json_data.get("rss_url")
                json_data = {"title": title, "rss_url": rss_url}
            else:  # PATCH
                blob_data, err = handle_blob_operation(
                    retry_with_backoff(
                        lambda: load_podcast_blob(podcast_id),
                        exceptions=(RuntimeError,),
                        max_attempts=3,
                        initial_delay=1.0,
                        backoff_factor=2.0
                    )
                )
                if err:
                    return error_response("Failed to load podcast data.", 404)
                json_data = json.loads(blob_data)
                if title:
                    json_data["title"] = title
                if rss_url:
                    json_data["rss_url"] = rss_url
                old_title = json.loads(blob_data).get("title")
                old_rss_url = json.loads(blob_data).get("rss_url")

            new_title = json_data.get("title")
            new_rss_url = json_data.get("rss_url")
            title_changed = bool(old_title and new_title and old_title != new_title)
            rss_changed = bool(old_rss_url and new_rss_url and old_rss_url != new_rss_url)

            title_reserved = False
            rss_reserved = False
            try:
                if title_changed:
                    create_podcast_index("title", new_title, podcast_id, overwrite=False)
                    title_reserved = True
                if rss_changed:
                    create_podcast_index("rss", new_rss_url, podcast_id, overwrite=False)
                    rss_reserved = True
            except PodcastIndexConflictError:
                if title_reserved:
                    try:
                        delete_podcast_index("title", new_title, expected_podcast_id=podcast_id)
                    except Exception:
                        pass
                return error_response("Podcast with this title or rss_url already exists.", 409)
            except Exception as e:
                logging.error(f"Failed to reserve updated podcast indexes: {e}", exc_info=True)
                return error_response("Failed to reserve updated podcast indexes.", 500)

            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: save_podcast_blob(json.dumps(json_data), podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                if title_reserved:
                    try:
                        delete_podcast_index("title", new_title, expected_podcast_id=podcast_id)
                    except Exception:
                        pass
                if rss_reserved:
                    try:
                        delete_podcast_index("rss", new_rss_url, expected_podcast_id=podcast_id)
                    except Exception:
                        pass
                return error_response("Failed to save podcast data.", 500)

            if title_changed:
                try:
                    delete_podcast_index("title", old_title, expected_podcast_id=podcast_id)
                except Exception:
                    logging.warning("Failed to delete old title index after update.", exc_info=True)
            if rss_changed:
                try:
                    delete_podcast_index("rss", old_rss_url, expected_podcast_id=podcast_id)
                except Exception:
                    logging.warning("Failed to delete old rss index after update.", exc_info=True)

            return json_response({
                "message": "Podcast updated successfully.",
                "result": {
                    "podcast_id": podcast_id,
                    "title": json_data.get("title"),
                    "rss_url": json_data.get("rss_url")
                }
            }, 200)
        except Exception as e:
            logging.error(f"Failed to update podcast: {e}", exc_info=True)
            return error_response("Failed to update podcast.", 500)

    elif req.method == "DELETE":
        try:
            blob_data, load_err = handle_blob_operation(
                retry_with_backoff(
                    lambda: load_podcast_blob(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=2,
                    initial_delay=0.5,
                    backoff_factor=2.0,
                )
            )
            old_title = None
            old_rss_url = None
            if not load_err and blob_data:
                try:
                    old_json = json.loads(blob_data)
                    old_title = old_json.get("title")
                    old_rss_url = old_json.get("rss_url")
                except Exception:
                    pass

            _, err = handle_blob_operation(
                retry_with_backoff(
                    lambda: delete_podcast_blob(podcast_id),
                    exceptions=(RuntimeError,),
                    max_attempts=3,
                    initial_delay=1.0,
                    backoff_factor=2.0
                )
            )
            if err:
                return error_response("Failed to delete podcast.", 500)

            if old_title:
                try:
                    delete_podcast_index("title", old_title, expected_podcast_id=podcast_id)
                except Exception:
                    logging.warning("Failed to delete title index during podcast delete.", exc_info=True)
            if old_rss_url:
                try:
                    delete_podcast_index("rss", old_rss_url, expected_podcast_id=podcast_id)
                except Exception:
                    logging.warning("Failed to delete rss index during podcast delete.", exc_info=True)

            return json_response({
                "message": "Podcast deleted successfully.",
                "result": {"podcast_id": podcast_id}
            }, 200)
        except Exception as e:
            logging.error(f"Failed to delete podcast: {e}", exc_info=True)
            return error_response("Failed to delete podcast.", 500)

    return error_response("Method Not Allowed", 405)
