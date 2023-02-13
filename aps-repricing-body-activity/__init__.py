# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from common import functions
import azure.functions as func
import json

def main(name: str) -> str:
    
    try:
        logging.info("Activity 3")
        url = "https://aps-others-parts-body-parts-functions.azurewebsites.net/api/durableBodyParts?code=96Hmvo1yLQe1pQTdzvIQUkk9S4XwiklNxD6habr1K8KFAzFuP9Tliw=="
        data = {}
        res = functions.apirequest(url,data)
        response = functions.durablevalidation(res.text)
        logging.info(response)
        return response
    except Exception as ex:
        logging.info(ex)
        response = {
            'code': 500,
            'text': "Body parts activity call failed."
        }
        return response
