# Builtins
from datetime import datetime
import time
import io

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
import contextlib
from selenium import webdriver
from selenium.webdriver.common.by import By
import boto3
import contextlib

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
        driver.set_window_size(1920, 1080)
        driver.get(self.endpoint)
        time.sleep(2)
        with contextlib.suppress(Exception):
            driver.find_element(By.CSS_SELECTOR,"#cookie-notifier-cta").click()
        page = 1
        while all or (max_pages is not None and page < max_pages):
            try:
                html_content = driver.page_source
                file_obj = io.BytesIO(html_content.encode())
                file_path = f'pipeline/raw/{self.type.lower()}/{self.city}/{datetime.now().date()}/{filename_pattern}-{page}.html'
                self.s3.upload_fileobj(file_obj, 'floriparentpricing', file_path)
                driver.execute_script("window.scrollTo(0,9000)")
                time.sleep(0.5)
                next_page = driver.find_element(By.XPATH, '//*[@id="js-site-main"]/div[2]/div[1]/section/div[2]/div[2]/div/ul/li[9]/button')
                page = int(next_page.get_attribute('data-page'))
                next_page.click()
            except Exception as e:
                print(f': An Exception Occurred: {e}')
                break
        return True
