import os
from datetime import datetime, date
from extractors import Extractor
from dotenv import load_dotenv
import boto3
load_dotenv()

s3= boto3.client('s3',
                aws_access_key_id = os.getenv('AWS_ID'),
                aws_secret_access_key=os.getenv('AWS_KEY'))

extractor = Extractor(cidade='florianopolis',
                   s3=s3) # Instanciando API

folder_path = 'pipeline/raw/vivareal/florianopolis/2023-11-04/'

extractor.process_folder(bucket_name = 'floriparentpricing',
                    folder_path=folder_path,
                    filename_pattern='processed',
                    output_format='parquet')