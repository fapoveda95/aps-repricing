# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import pandas as pd
from common import filefunctions as filefunctions

container = "apsrepricing-files"
mechanical_reprice_filename = "mechanical_unique_reprice.csv"
noic_reprice_filename = "noicparts_unique_reprice.csv"
body_reprice_filename = "bodyparts_unique_reprice.csv"
nonbody_reprice_filename = "nonbodyparts_unique_reprice.csv"
minprice_reprice_filename = "mingreater_unique_reprice.csv"

def main(name: str) -> str:
    try:
        logging.info("Activity 5")
        mechanicalfile = filefunctions.read_reprice_file(mechanical_reprice_filename, container)
        noicfile = filefunctions.read_reprice_file(noic_reprice_filename, container)
        bodyfile = filefunctions.read_reprice_file(body_reprice_filename, container)
        nonbodyfile = filefunctions.read_reprice_file(nonbody_reprice_filename, container)
        minpricefile = filefunctions.read_reprice_file(minprice_reprice_filename, container)
        logging.info('Files download succesfully')
        mainfile = pd.DataFrame()
        mainfile['unique'] = None
        mainfile['reprice'] = None
        repricefile = mainfile.append([mechanicalfile, bodyfile, nonbodyfile, noicfile, minpricefile], ignore_index=True)
        reprice_csv_path = filefunctions.saveToCSV_no_header(repricefile, 'aps_unique_reprice.csv')
        filefunctions.uploadToBlobStorage(container,'aps_unique_reprice.csv',reprice_csv_path)
        logging.info('Files upload successfully')
        response = {
            'code': 200,
            'text': "File upload function executed successfully."
        }
        return response
    except Exception as ex:
        logging.info(ex)
        response = {
            'code': 500,
            'text': "File upload function failed."
        }
        return response
