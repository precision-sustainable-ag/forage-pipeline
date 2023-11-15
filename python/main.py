import io
from helpers import read_blob_to_pandas
import requests
import pandas as pd
import numpy as np
from helpers import ONFARM_KEY, CREDENTIAL, ACCOUNT_URL
from azure.storage.blob import ContainerClient

#### read dite data from azure blob ####
df = read_blob_to_pandas()

unique_sites = np.unique(df.site_code)
unique_sites = ",".join(unique_sites)

#### Get biomass data from onfarm ####
url = f"https://api.precisionsustainableag.org/onfarm/biomass?subplot=separate&code={unique_sites}"
headers = {
    'x-api-key': ONFARM_KEY,
}
r=requests.get(url, headers=headers)
result = r.json()
biomass_df = pd.DataFrame(result)
biomass_df = biomass_df.rename({
    "code": "site_code", 
    "subplot": "replicate",
}, axis=1)
biomass_df['replicate'] = biomass_df['replicate'].astype('int')
biomass_df['site_code'] = biomass_df['site_code'].astype('str')
df['replicate'] = df['replicate'].astype('int')
df['site_code'] = df['site_code'].astype('str')
joined_df = pd.merge(biomass_df, df, how='inner', on=['site_code','replicate'])

### write csv to azure blob 
output = io.StringIO()
output = joined_df.to_csv(encoding = "utf-8")
blob_container_client_4 = ContainerClient(account_url=ACCOUNT_URL, container_name="04-plots-with-biomass", credential=CREDENTIAL)
blob_container_client_4.upload_blob('forage_box_onfarm.csv', output, overwrite=True, encoding='utf-8')
