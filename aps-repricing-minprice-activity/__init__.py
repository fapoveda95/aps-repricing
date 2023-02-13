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
        url = "https://aps-reprice-minimum-greater-than-current-category.azurewebsites.net/api/aps-reprice-minimum-greater-than-current-category?code=qW43YHx1jETw6hm2jPKnUlwsnwD4YIuyUNwUyUKJej0CAzFumf7bNA=="
        data = {}
        res = functions.apirequest(url,data)
        logging.info(res)
        response = {
            'code': res.status_code,
            'text': res.text
        }
        return response
    except Exception as ex:
        logging.info(ex)
        response = {
            'code': 500,
            'text': "APS Reprice Minimum Greater Than Current Category activity failed."
        }
        return response


