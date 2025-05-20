import logging
import azure.functions as func
from utils.azure_blob import blob_container_client
from utils.retry import retry_with_backoff
import pandas as pd
import numpy as np
import statsmodels.api as sm
import json
from typing import Optional

def impact(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint to calculate the impact of episode releases using regression analysis.
    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: The HTTP response with regression results or error message.
    """
    logging.info("Received request to calculate episode impact using regression.")

    try:
        # Validate HTTP method
        if req.method != "GET":
            logging.error(f"Invalid HTTP method: {req.method}")
            return func.HttpResponse(
                "Invalid HTTP method. Only GET requests are allowed.",
                status_code=405
            )

        # Extract and validate inputs
        token: Optional[str] = req.params.get('token')
        if not token:
            error_message = "Missing 'token' in the request."
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)

        # Retrieve JSON from Blob Storage with retry
        try:
            def load_blob():
                blob_name = f"{token}.json"
                blob_client = blob_container_client.get_blob_client(blob_name)
                return blob_client.download_blob().readall().decode('utf-8')
            json_data = retry_with_backoff(
                load_blob,
                exceptions=(Exception,),
                max_attempts=3,
                initial_delay=1.0,
                backoff_factor=2.0
            )()
        except Exception as e:
            error_message = f"Error retrieving data for token {token}: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse("Error retrieving data from storage.", status_code=404)

        # Parse JSON into DataFrame
        try:
            df = pd.read_json(json_data, orient="records")
            df['Date'] = pd.to_datetime(df['Date'])
            df.sort_values('Date', inplace=True)
        except Exception as e:
            error_message = f"Error parsing dataset: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse("Error parsing dataset.", status_code=400)

        # Add columns for episodes released in the past 0–7 days
        try:
            max_days = 7
            for i in range(max_days + 1):
                col_name = f"Episodes released today-{i}"
                df[col_name] = df['Episodes Released'].shift(i).fillna(0)
        except Exception as e:
            logging.error(f"Error adding lag columns: {e}", exc_info=True)
            return func.HttpResponse("Error preparing features for regression.", status_code=500)

        # Prepare predictors (X) and response (y)
        try:
            predictors = [f"Episodes released today-{i}" for i in range(max_days + 1)]
            X = df[predictors]
            y = df['Downloads']
        except Exception as e:
            logging.error(f"Error preparing predictors/response: {e}", exc_info=True)
            return func.HttpResponse("Error preparing regression data.", status_code=500)

        # Fit linear regression model
        try:
            X = sm.add_constant(X)  # Add intercept
            model = sm.OLS(y, X).fit()
            logging.info("Regression Model Summary:\n" + str(model.summary()))
        except Exception as e:
            logging.error(f"Error fitting regression model: {e}", exc_info=True)
            return func.HttpResponse("Error fitting regression model.", status_code=500)

        # Analyze results
        try:
            results = []
            significance_level = 0.05
            for i, predictor in enumerate(predictors):
                coef = model.params[predictor]
                p_value = model.pvalues[predictor]
                if p_value < significance_level:
                    results.append({
                        'day_offset': i,
                        'impact': coef,
                        'p_value': p_value
                    })
                else:
                    break
        except Exception as e:
            logging.error(f"Error analyzing regression results: {e}", exc_info=True)
            return func.HttpResponse("Error analyzing regression results.", status_code=500)

        # Summarize results
        try:
            if results:
                days_of_impact = len(results)
                average_impact = float(np.mean([result['impact'] for result in results]))
                impact_per_day = [
                    {
                        'day_offset': int(result['day_offset']),
                        'impact': float(result['impact'])
                    } for result in results
                ]
            else:
                days_of_impact = 0
                average_impact = 0.0
                impact_per_day = []
        except Exception as e:
            logging.error(f"Error summarizing regression results: {e}", exc_info=True)
            return func.HttpResponse("Error summarizing regression results.", status_code=500)

        # Prepare response
        try:
            response = {
                'days_of_impact': days_of_impact,
                'average_impact': average_impact,
                'impact_per_day': impact_per_day
            }
            return func.HttpResponse(
                json.dumps(response),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.error(f"Error preparing response: {e}", exc_info=True)
            return func.HttpResponse("Error preparing response.", status_code=500)

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            "An unexpected error occurred.",
            status_code=500
        )
