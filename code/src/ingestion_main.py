import os
from selenium import webdriver
from datetime import datetime, date
from ingestors import Ingestor
from dotenv import load_dotenv
import boto3
load_dotenv()

s3= boto3.client('s3',
                    aws_access_key_id = os.getenv('AWS_ID'),
                    aws_secret_access_key=os.getenv('AWS_KEY'))

driver = webdriver.Chrome() #Instanciando Driver

ingestor = Ingestor(webdriver=driver,
                   cidade='florianopolis',
                   s3=s3) # Instanciando API

 # Chamando a função de ingestão
ingestor.ingest_pages(
    filename_pattern='page',
    delay_seconds=1.4,
    all=True)
