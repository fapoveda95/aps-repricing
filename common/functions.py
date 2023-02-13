import requests
import json
import logging

def apirequest(endpoint, data):    
    res = requests.post(url = endpoint, json = data)
    return res

def durablevalidation(message):
    a = ""
    a = a + message
    path = a[a.find("https"):len(a)]
    res = requests.get(url=path)
    data = json.loads(res.text)
    logging.info("Data", data)
    status = data["runtimeStatus"]
    while ((status == "Running")|(status == "Pending")):
        res = requests.get(url=path)
        data = json.loads(res.text)
        status = data["runtimeStatus"]
    logging.info("Data",data)
    response = data["output"][0]
    return response

def execute_repricing_process():
    url = "https://aps-repricing.azurewebsites.net/api/orchestrators/aps-repricing-orchestrator"   
    r = requests.get(url = url)
    data = r.json()
    status = data['statusQueryGetUri']
    return status