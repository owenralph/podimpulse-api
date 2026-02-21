import azure.functions as func
import json
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
    return initialize_handler(req)

@app.route(route="v1/rss")
def rss(req: func.HttpRequest) -> func.HttpResponse:
    return rss_handler(req)

@app.route(route="v1/ingest")
def ingest(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/ingest")

@app.route(route="v1/missing")
def missing(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/missing")

@app.route(route="v1/trend")
def trend(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/trend")

@app.route(route="v1/impact")
def impact(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/impact")

@app.route(route="v1/analyze_regression")
def analyze_regression(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/regression")

@app.route(route="v1/predict")
def predict_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    return _legacy_route_gone("/v1/podcasts/{podcast_id}/predict")

"""""""""
Facebook connection
"""""""""
@app.route(route="v1/facebook/exchange_user_token", methods=["POST"])
def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    return exchange_user_token_handler(req)

@app.route(route="v1/facebook/get_user_pages", methods=["POST"])
def get_user_pages(req: func.HttpRequest) -> func.HttpResponse:
    return get_user_pages_handler(req)

@app.route(route="v1/facebook/get_page_token", methods=["POST"])
def get_page_token(req: func.HttpRequest) -> func.HttpResponse:
    return get_page_token_handler(req)

@app.route(route="v1/facebook/query_page_analytics", methods=["POST"])
def query_page_analytics(req: func.HttpRequest) -> func.HttpResponse:
    return query_page_analytics_handler(req)

"""""""""
Podcasts
"""""""""
# Podcasts collection endpoints
@app.route(route="v1/podcasts", methods=["POST", "GET"])
def podcasts_collection(req: func.HttpRequest) -> func.HttpResponse:
    # Implemented in functions.v1.initialize or a new handler as needed
    return initialize_handler(req)

# Podcast resource endpoints
@app.route(route="v1/podcasts/{podcast_id}", methods=["GET", "PUT", "PATCH", "DELETE"])
def podcast_resource(req: func.HttpRequest) -> func.HttpResponse:
    from functions.v1.initialize import podcast_resource as podcast_resource_handler
    return podcast_resource_handler(req)

# Ingest endpoints
@app.route(route="v1/podcasts/{podcast_id}/ingest", methods=["POST", "GET", "DELETE"])
def podcast_ingest(req: func.HttpRequest) -> func.HttpResponse:
    return ingest_handler(req)

# Missing episodes endpoints
@app.route(route="v1/podcasts/{podcast_id}/missing", methods=["GET", "POST"])
def podcast_missing(req: func.HttpRequest) -> func.HttpResponse:
    return missing_handler(req)

# Predict endpoints
@app.route(route="v1/podcasts/{podcast_id}/predict", methods=["POST", "GET"])
def podcast_predict(req: func.HttpRequest) -> func.HttpResponse:
    return predict_handler(req)

# Regression endpoints
@app.route(route="v1/podcasts/{podcast_id}/regression", methods=["POST", "GET"])
def podcast_regression(req: func.HttpRequest) -> func.HttpResponse:
    return analyze_regression_handler(req)

# Trend endpoints
@app.route(route="v1/podcasts/{podcast_id}/trend", methods=["GET"])
def podcast_trend(req: func.HttpRequest) -> func.HttpResponse:
    return trend_handler(req)

# Impact endpoint
@app.route(route="v1/podcasts/{podcast_id}/impact", methods=["GET"])
def podcast_impact(req: func.HttpRequest) -> func.HttpResponse:
    return impact_handler(req)
