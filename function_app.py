import azure.functions as func
import json
import logging
import time
import uuid
from typing import Callable
from functions.v1.initialize import initialize as initialize_handler
from functions.v1.rss import rss as rss_handler
from functions.v1.ingest import ingest as ingest_handler
from functions.v1.missing import missing as missing_handler
from functions.v1.trend import trend as trend_handler
from functions.v1.impact import impact as impact_handler
from functions.v1.facebook.token import get_page_token as get_page_token_handler, exchange_user_token as exchange_user_token_handler
from functions.v1.facebook.pages import get_user_pages as get_user_pages_handler
from functions.v1.facebook.analytics import query_reels_analytics as query_page_analytics_handler
from functions.v1.regression import regression as analyze_regression_handler
from functions.v1.predict import predict as predict_handler

# Initialize the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
LEGACY_ROUTE_REMOVAL_DATE = "2026-06-30"


def _invoke_with_metrics(
    req: func.HttpRequest, route_name: str, handler: Callable[[func.HttpRequest], func.HttpResponse]
) -> func.HttpResponse:
    start = time.perf_counter()
    request_id = (
        req.headers.get("x-request-id")
        or req.headers.get("x-ms-request-id")
        or uuid.uuid4().hex
    )
    status_code = 500
    try:
        response = handler(req)
        status_code = getattr(response, "status_code", 200)
        return response
    except Exception:
        logging.exception(
            "[metric] request.exception route=%s method=%s request_id=%s",
            route_name,
            getattr(req, "method", "UNKNOWN"),
            request_id,
        )
        raise
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        logging.info(
            "[metric] request route=%s method=%s status=%s duration_ms=%.2f request_id=%s",
            route_name,
            getattr(req, "method", "UNKNOWN"),
            status_code,
            duration_ms,
            request_id,
        )


def _legacy_route_gone(replacement: str) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(
            {
                "message": (
                    f"This endpoint is deprecated and will be removed after {LEGACY_ROUTE_REMOVAL_DATE}. "
                    f"Use {replacement} instead."
                ),
                "result": {
                    "replacement": replacement,
                    "sunset_date": LEGACY_ROUTE_REMOVAL_DATE,
                },
            }
        ),
        status_code=410,
        mimetype="application/json",
        headers={
            "Deprecation": "true",
            "Sunset": "Tue, 30 Jun 2026 23:59:59 GMT",
            "Link": f"<{replacement}>; rel=\"alternate\"",
        },
    )

"""""""""
Preparation
"""""""""
@app.route(route="v1/initialize")
def initialize(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/initialize", initialize_handler)

@app.route(route="v1/rss")
def rss(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/rss", rss_handler)

@app.route(route="v1/ingest")
def ingest(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/ingest", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/ingest")
    )

@app.route(route="v1/missing")
def missing(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/missing", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/missing")
    )

@app.route(route="v1/trend")
def trend(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/trend", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/trend")
    )

@app.route(route="v1/impact")
def impact(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/impact", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/impact")
    )

@app.route(route="v1/analyze_regression")
def analyze_regression(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/analyze_regression", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/regression")
    )

@app.route(route="v1/predict")
def predict_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(
        req, "v1/predict", lambda _req: _legacy_route_gone("/v1/podcasts/{podcast_id}/predict")
    )

"""""""""
Facebook connection
"""""""""
@app.route(route="v1/facebook/exchange_user_token", methods=["POST"])
def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/facebook/exchange_user_token", exchange_user_token_handler)

@app.route(route="v1/facebook/get_user_pages", methods=["POST"])
def get_user_pages(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/facebook/get_user_pages", get_user_pages_handler)

@app.route(route="v1/facebook/get_page_token", methods=["POST"])
def get_page_token(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/facebook/get_page_token", get_page_token_handler)

@app.route(route="v1/facebook/query_page_analytics", methods=["POST"])
def query_page_analytics(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/facebook/query_page_analytics", query_page_analytics_handler)

"""""""""
Podcasts
"""""""""
# Podcasts collection endpoints
@app.route(route="v1/podcasts", methods=["POST", "GET"])
def podcasts_collection(req: func.HttpRequest) -> func.HttpResponse:
    # Implemented in functions.v1.initialize or a new handler as needed
    return _invoke_with_metrics(req, "v1/podcasts", initialize_handler)

# Podcast resource endpoints
@app.route(route="v1/podcasts/{podcast_id}", methods=["GET", "PUT", "PATCH", "DELETE"])
def podcast_resource(req: func.HttpRequest) -> func.HttpResponse:
    from functions.v1.initialize import podcast_resource as podcast_resource_handler
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}", podcast_resource_handler)

# Ingest endpoints
@app.route(route="v1/podcasts/{podcast_id}/ingest", methods=["POST", "GET", "DELETE"])
def podcast_ingest(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/ingest", ingest_handler)

# Missing episodes endpoints
@app.route(route="v1/podcasts/{podcast_id}/missing", methods=["GET", "POST"])
def podcast_missing(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/missing", missing_handler)

# Predict endpoints
@app.route(route="v1/podcasts/{podcast_id}/predict", methods=["POST", "GET"])
def podcast_predict(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/predict", predict_handler)

# Regression endpoints
@app.route(route="v1/podcasts/{podcast_id}/regression", methods=["POST", "GET"])
def podcast_regression(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/regression", analyze_regression_handler)

# Trend endpoints
@app.route(route="v1/podcasts/{podcast_id}/trend", methods=["GET"])
def podcast_trend(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/trend", trend_handler)

# Impact endpoint
@app.route(route="v1/podcasts/{podcast_id}/impact", methods=["GET"])
def podcast_impact(req: func.HttpRequest) -> func.HttpResponse:
    return _invoke_with_metrics(req, "v1/podcasts/{podcast_id}/impact", impact_handler)
