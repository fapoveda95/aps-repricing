import requests
import json
import logging
import azure.functions as func
from common import functions as functions

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        status = functions.execute_repricing_process()
        return func.HttpResponse(f"We inform you that the Repricing Process started successfully. To view status, click on this link: {status}", status_code=200)
    except Exception as ex:
        return func.HttpResponse(f"Error: {ex}", status_code=400)
        