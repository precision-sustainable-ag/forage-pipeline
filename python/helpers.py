import os
import io
import requests
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
load_dotenv()

CONN_STR = os.getenv('CONNECTION_STRING')
ONFARM_KEY = os.getenv('ONFARM_KEY')
CREDENTIAL = os.getenv('CREDENTIAL')
ACCOUNT_URL = os.getenv('ACCOUNT_URL')

def download_blob_to_pandas(client, blob_name):
    downloaded_blob = client.download_blob(blob_name, encoding='utf8')
    df = pd.read_csv(StringIO(downloaded_blob.readall()), low_memory=False)
    return df

def parse_blob_name(blob_name):
    box_name, experiment_type, site_code, timing, species, replicate, f_type_indic, date_str, uuid = blob_name.split("/")[-1].split(".")[0].split("_")
    timestamp = datetime.strptime(date_str, "%Y%m%d")
    return {
        "box_name": box_name,
        "experiment_type": experiment_type,
        "site_code": site_code,
        "timing": timing,
        "species": species,
        "replicate": replicate,
        "f_type_indic": f_type_indic,
        "date_str": date_str,
        "uuid": uuid,
        "timestamp": timestamp,
    }
    
def read_blob_to_pandas():
    blob_service_client = BlobServiceClient(account_url="https://boxfiles.blob.core.windows.net")
    blob_service_client = blob_service_client.from_connection_string(conn_str=CONN_STR)
    blob_container_client_3 = blob_service_client.get_container_client("03-plots-with-summary")

    ## List all blobs in the container
    blob_list = blob_container_client_3.list_blobs()
    filtered_blobs = []
    for blob in blob_list:
        fname = blob.name
        if not 'onfarm' in fname:
            continue
        box_name, experiment_type, site_code, timing, species, replicate, f_type_indic, date_str, uuid = fname.split(".")[0].split("_")
        timestamp = datetime.strptime(date_str, "%Y%m%d")
        if experiment_type == "onfarm" and timing == "biomass":
            filtered_blobs.append(blob)
            
    blob = filtered_blobs[0]
    df_list = [download_blob_to_pandas(blob_container_client_3, el.name) for el in filtered_blobs]
    df_agg = pd.concat(df_list, axis=0)
    df_props = [parse_blob_name(fname) for fname in list(df_agg.iloc[:,0])]
    df_props = pd.DataFrame(df_props)
    df_props.reset_index(inplace=True, drop=True)
    df_agg.reset_index(inplace=True, drop=True)
    df = pd.concat([df_agg, df_props], axis=1)
    df = df.loc[df.flag!=0]
    df.reset_index(inplace=True, drop=True)
    df['replicate'] = [int(el) for _,el in df['replicate'].str.split("rep")]
    return df