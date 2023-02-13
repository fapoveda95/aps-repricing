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


def main(name: str) -> str:
    try:
        logging.info("Activity 2")
        url = "https://aps-mech-time-on-inventory-repricing.azurewebsites.net/api/aps-repricing-mechanical-time-on-inventory"
        data = {}
        res = functions.apirequest(url,data)
        response = functions.durablevalidation(res.text)
        logging.info(response)
        return response
    except Exception as ex:
        logging.info(ex)
        response = {
            'code': 500,
            'text': "Mechanical activity call failed."
        }
        return response
        