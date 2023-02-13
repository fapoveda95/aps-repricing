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
        logging.info("Activity 4")
        url = "https://aps-others-parts-non-body-parts-functions.azurewebsites.net/api/durableNonBodyParts?code=3RAWHXajt0ex1EcI12Ympv5B5fO0Ky4x0m9NNm9Yu3SnAzFuWsqgWQ=="
        data = {}
        res = functions.apirequest(url,data)
        response = functions.durablevalidation(res.text)
        logging.info(response)
        return response
    except Exception as ex:
        logging.info(ex)
        response = {
            'code': 500,
            'text': "Non body parts activity call failed."
        }
        return response
