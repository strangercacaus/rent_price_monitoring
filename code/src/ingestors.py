# Builtins
from datetime import datetime
import time
import io

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
from selenium import webdriver
from selenium.webdriver.common.by import By
import boto3

# Módulos Personalizados
from utils import ResultSet

class Ingestor():

    def __init__(self, cidade:str, webdriver:webdriver = None, s3:boto3.client = None) -> None:
        self.city = cidade
        self.s3 = s3
        self.webdriver = webdriver
        self.type = 'Vivareal'
        

    @property
    def endpoint(self) -> str:
        """
        Obtém o endpoint da API.

        Retorna:
            Uma string representando o endpoint base da API.
        """
        return f'https://www.vivareal.com.br/aluguel/santa-catarina/{self.city}/'
    

    def ingest_pages(self, filename_pattern:str, all:bool=True, max_pages:int=None, delay_seconds:int=0) -> None:
        """
        Ingere várias páginas de dados da API e salva o conteúdo HTML em arquivos na camada RAW.

        Args:
            output_path (str): O caminho para o diretório onde os arquivos HTML serão salvos.
            filename_pattern (str): O padrão para os nomes dos arquivos HTML salvos.
            all (bool, opcional): Um booleano indicando se todas as páginas disponíveis devem ser ingeridas (padrão é True).
            pages (int, opcional): O número máximo de páginas a serem ingeridas (padrão é None).

        Returns:
            None.

        Raises:
            None.

        """
        driver = self.webdriver
        driver.set_window_size(1366, 800)
        driver.get(self.endpoint)
        page = 1
        while all or (max_pages is not None and page < max_pages):
            try:
                html_content = driver.page_source
                file_obj = io.BytesIO(html_content.encode())
                file_path = f'pipeline/raw/{self.type.lower()}/{self.city}/{datetime.now().date()}/{filename_pattern}-{page}.html'
                self.s3.upload_fileobj(file_obj, 'floriparentpricing', file_path)
                driver.execute_script("window.scrollTo(0,9800)")
                driver.find_element(By.CSS_SELECTOR, ".pagination__item:nth-child(9) > .js-change-page").click()
                time.sleep(delay_seconds)
                page += 1
            except Exception as e:
                print(f': An Exception Occurred: {e}')
                break
        return True
