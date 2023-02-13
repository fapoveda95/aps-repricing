# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    responses = []
    result1 = yield context.call_activity('aps-repricing-noic-activity', "aps-repricing-noic-activity")
    responses.append(result1)
    if(result1["code"] == 200):
        logging.info("Activity response 1:",result1)
        result2 = yield context.call_activity('aps-repricing-mechanical-activity', "aps-repricing-mechanical-activity")
        responses.append(result2)
        if(result2["code"] == 200):
            logging.info("Activity response 2:",result2)
            result3 = yield context.call_activity('aps-repricing-body-activity', "aps-repricing-body-activity")
            responses.append(result3)
            if(result3["code"] == 200):
                logging.info("Activity response 3:",result3)
                result4 = yield context.call_activity('aps-repricing-nonbody-activity', "aps-repricing-nonbody-activity")
                responses.append(result4)
                if(result4["code"] == 200):
                    logging.info("Activity response 4:",result4)
                    result5 = yield context.call_activity('aps-repricing-minprice-activity', "aps-repricing-minprice-activity")
                    responses.append(result5)
                    if(result5["code"] == 200):
                        logging.info("Activity response 5:",result5)
                        result6 = yield context.call_activity('aps-repricing-file-activity', "aps-repricing-file-activity")
                        responses.append(result6)
                        if(result6["code"] == 200):
                            logging.info("Activity response 6:",result6)
                            return responses
                        else:
                            logging.info("Activity error 6", result6)
                            return responses
                    else:
                        logging.info("Activity error 5", result5)
                        return responses
                else:
                    logging.info("Activity error 4", result4)
                    return responses
            else:
                logging.info("Activity error 3", result3)
                return responses
        else:
            logging.info("Activity error 2", result2)
            return responses
    else:
        logging.info("Activity error 1", result1)
        return responses

"""def orchestrator_function(context: df.DurableOrchestrationContext):
    responses = []
    result1 = yield context.call_activity('aps-repricing-nonbody-activity', "aps-repricing-nonbody-activity")
    responses.append(result1)
    if(result1["code"] == 200):
        logging.info("Activity response 1:",result1)
        result2 = yield context.call_activity('aps-repricing-file-activity', "aps-repricing-file-activity")
        responses.append(result2)
        if(result2["code"] == 200):
            logging.info("Activity response 2:",result2)
            return responses
        else:
            logging.info("Activity error 2", result2)
            return responses
    else:
        logging.info("Activity error 1", result1)
        return responses"""


main = df.Orchestrator.create(orchestrator_function)