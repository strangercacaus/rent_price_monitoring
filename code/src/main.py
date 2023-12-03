def extract(aws_id=None, aws_key=None,date='2023-11-28'):
    from extractors import Extractor
    import boto3

    s3= boto3.client('s3',
                    aws_access_key_id = aws_id,
                    aws_secret_access_key= aws_key)

    extractor = Extractor(cidade='florianopolis',
                    s3=s3) # Instanciando API

    folder_base_path = 'pipeline/raw/vivareal/florianopolis/'
    folder_path = f'{folder_base_path}{date}'

    extractor.process_folder(bucket_name = 'floriparentpricing',
                        folder_path=folder_path,
                        filename_pattern='processed',
                        output_format='parquet')
    
def ingest(aws_id=None, aws_key=None):
    from selenium import webdriver
    from ingestors import Ingestor
    import boto3
    
    s3= boto3.client('s3',
                    aws_access_key_id = aws_id,
                    aws_secret_access_key= aws_key)

    driver = webdriver.Chrome() #Instanciando Driver

    ingestor = Ingestor(webdriver=driver,
                    cidade='florianopolis',
                    s3=s3) # Instanciando API

    # Chamando a função de ingestão
    ingestor.ingest_pages(
        filename_pattern='page',
        delay_seconds=1.4,
        all=True)