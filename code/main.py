#%% Ingestão

import os
from selenium import webdriver
from datetime import datetime, date
from apis import VivaRealApi
from dotenv import load_dotenv
import boto3
load_dotenv()

s3= boto3.client('s3',
                                    aws_access_key_id = os.getenv('AWS_ID'),
                                    aws_secret_access_key=os.getenv('AWS_KEY'))

driver = webdriver.Chrome() #Instanciando Driver

viva = VivaRealApi(webdriver=driver,
                   cidade='florianopolis',
                   s3=s3) # Instanciando API

 # Chamando a função de ingestão
viva.ingest_pages(
    filename_pattern='page',
    delay_seconds=1.4,
    all=True)


#%% Processamento
import boto3

s3

#%%
import os
from datetime import datetime, date
from apis import VivaRealApi
from dotenv import load_dotenv
import boto3
load_dotenv()

s3= boto3.client('s3',
                aws_access_key_id = os.getenv('AWS_ID'),
                aws_secret_access_key=os.getenv('AWS_KEY'))

viva = VivaRealApi(cidade='florianopolis',
                   s3=s3) # Instanciando API

folder_path = 'pipeline/raw/vivareal/florianopolis/2023-11-04/'

viva.process_folder(bucket_name = 'floriparentpricing',
                    folder_path=folder_path,
                    filename_pattern='processed',
                    output_format='parquet')


# %% Consolidação
