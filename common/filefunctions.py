import logging
import azure.functions as func
import tempfile
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient


MY_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=standardpricingstorage;AccountKey=aNkJlZtoz/HEjBwEfM4uYAcDd39JQlkyWS+10795Du/y+K0boexXPtg19mUOjnZBrJes+ESpZQGG+AStpA3e5g==;EndpointSuffix=core.windows.net"

def uploadToBlobStorage(containerName,fileName,upload_file_path):

            blob_service_client = BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(container=containerName, blob=fileName)
            
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data,overwrite=True, connection_timeout=14400)
                print(f"uploading file - {fileName}")

LOCAL_BLOB_PATH = tempfile.gettempdir()

def download_blob(file_name, container):
    blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
    my_container = blob_service_client.get_container_client(container)
    print(file_name)
    bytes = my_container.get_blob_client(file_name).download_blob().readall()
    # Get full path to the file
    download_file_path = os.path.join(LOCAL_BLOB_PATH, file_name)
    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
    with open(download_file_path, "wb") as file:
        file.write(bytes)
        return download_file_path

def saveToCSV_no_header(df, fileName):
    file_path = os.path.join(LOCAL_BLOB_PATH, fileName)
    df.to_csv(file_path, index=False, header = False)
    return file_path

def read_reprice_file(file_name, container):
    file_path = download_blob(file_name, container)
    reprice_file = pd.read_csv(file_path)
    file = pd.DataFrame()
    file['unique'] = None
    file['reprice'] = None
    if reprice_file.shape[0] > 0:
        repricefile = pd.read_csv(file_path, header = None, names=["unique","reprice"])
        file = file.append(repricefile,ignore_index=True)
    return file 
