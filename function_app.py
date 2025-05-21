import azure.functions as func
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

# Initialize the Function App
# I hope this works
app = func.FunctionApp()

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
    return ingest_handler(req)

@app.route(route="v1/missing")
def missing(req: func.HttpRequest) -> func.HttpResponse:
    return missing_handler(req)

@app.route(route="v1/trend")
def trend(req: func.HttpRequest) -> func.HttpResponse:
    return trend_handler(req)

@app.route(route="v1/impact")
def impact(req: func.HttpRequest) -> func.HttpResponse:
    return impact_handler(req)

@app.route(route="v1/analyze_regression")
def analyze_regression(req: func.HttpRequest) -> func.HttpResponse:
    return analyze_regression_handler(req)

"""""""""
Facebook connection
"""""""""
@app.route(route="v1/facebook/exchange_user_token")
def exchange_user_token(req: func.HttpRequest) -> func.HttpResponse:
    return exchange_user_token_handler(req)

@app.route(route="v1/facebook/get_user_pages")
def get_user_pages(req: func.HttpRequest) -> func.HttpResponse:
    return get_user_pages_handler(req)

@app.route(route="v1/facebook/get_page_token")
def get_page_token(req: func.HttpRequest) -> func.HttpResponse:
    return get_page_token_handler(req)

@app.route(route="v1/facebook/query_page_analytics")
def query_page_analytics(req: func.HttpRequest) -> func.HttpResponse:
    return query_page_analytics_handler(req)