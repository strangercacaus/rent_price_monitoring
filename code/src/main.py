def extract(date='2023-11-28',s3=None):
    from extractors import Extractor

    extractor = Extractor(cidade='florianopolis',
                    s3=s3) # Instanciando API

    folder_base_path = 'pipeline/raw/vivareal/florianopolis/'
    folder_path = f'{folder_base_path}{date}'

    extractor.process_folder(bucket_name = 'floriparentpricing',
                        folder_path=folder_path,
                        filename_pattern='processed',
                        output_format='parquet')
    
def ingest(driver=None, s3=None, all=True, max_pages=None):
    from ingestors import Ingestor

    ingestor = Ingestor(webdriver=driver,
                    cidade='florianopolis',
                    s3=s3) # Instanciando API

    # Chamando a função de ingestão
    ingestor.ingest_pages(
        filename_pattern='page',
        delay_seconds=1.4,
        all=all,
        max_pages=max_pages)

