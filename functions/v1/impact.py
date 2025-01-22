import logging
import azure.functions as func
from utils.azure_blob import blob_container_client
import pandas as pd
import numpy as np
import statsmodels.api as sm
import json


def impact(req: func.HttpRequest) -> func.HttpResponse:
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
        token = req.params.get('token')
        if not token:
            error_message = "Missing 'token' in the request."
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)

        # Retrieve JSON from Blob Storage
        try:
            blob_name = f"{token}.json"
            blob_client = blob_container_client.get_blob_client(blob_name)
            json_data = blob_client.download_blob().readall().decode('utf-8')
        except Exception as e:
            error_message = f"Error retrieving data for token {token}: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=404)

        # Parse JSON into DataFrame
        try:
            df = pd.read_json(json_data, orient="records")
            df['Date'] = pd.to_datetime(df['Date'])
            df.sort_values('Date', inplace=True)
        except Exception as e:
            error_message = f"Error parsing dataset: {e}"
            logging.error(error_message, exc_info=True)
            return func.HttpResponse(error_message, status_code=400)

        # Add columns for episodes released in the past 0–7 days
        max_days = 7
        for i in range(max_days + 1):
            col_name = f"Episodes released today-{i}"
            df[col_name] = df['Episodes Released'].shift(i).fillna(0)

        # Prepare predictors (X) and response (y)
        predictors = [f"Episodes released today-{i}" for i in range(max_days + 1)]
        X = df[predictors]
        y = df['Downloads']

        # Fit linear regression model
        X = sm.add_constant(X)  # Add intercept
        model = sm.OLS(y, X).fit()

        # Log the full regression results
        logging.info("Regression Model Summary:\n" + str(model.summary()))

        # Analyze results
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


        # Summarize results
        if results:
            days_of_impact = len(results)
            average_impact = np.mean([result['impact'] for result in results])
            impact_per_day = [{
                'day_offset': result['day_offset'],
                'impact': result['impact']
            } for result in results]
        else:
            days_of_impact = 0
            average_impact = 0
            impact_per_day = []

        # Prepare response
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
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(
            f"An unexpected error occurred: {str(e)}",
            status_code=500
        )
