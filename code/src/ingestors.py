# Builtins
from datetime import datetime
import time
import io
import logging

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
import contextlib

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
import boto3
import contextlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Add a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class Ingestor():

    def __init__(self, cidade:str, estado:str, bucket:str, webdriver:webdriver = None, s3:boto3.client = None) -> None:
        self.city = cidade
        self.state = estado
        self.s3 = s3
        self.bucket = bucket
        self.webdriver = webdriver
        self.type = 'Vivareal'
        

    @property
    def endpoint(self) -> str:
        """
        Obtém o endpoint da API.

        Retorna:
            Uma string representando o endpoint base da API.
        """
        city = self.city.strip().lower()
        state = self.state.strip().lower()
        return f'https://www.vivareal.com.br/aluguel/{state}/{city}/'
    

    def ingest_pages(self, filename_pattern: str, all: bool = True, max_pages: int = None, delay_seconds: int = 0) -> None:
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
        
        if all and max_pages is not None:
            raise ValueError("Cannot set 'all' to True while also specifying 'max_pages'")

        # Guarda o webdriver da classe em uma variável
        driver = self.webdriver

        # Configura o tamanho da tela para coincidir com a posição da navegação futura
        driver.set_window_size(1920, 1080)

        # Acessa a página inicial da rotina
        driver.get(self.endpoint)

        # Aguarda todos os elementos e popups serem carregados
        time.sleep(2)

        # Se houver popup, clica para fechar, se não, suprime as exceções do selenium.
        with contextlib.suppress(Exception):
            driver.find_element(By.CSS_SELECTOR, "#cookie-notifier-cta").click()

        # Inicia o contador de página inicial
        page = 1

        # Realiza um loop até não haver mais páginas disponíveis ou chegar ao máximo definido em max_pages
        while all or (max_pages is not None and page <= max_pages):
            try:

                next_page = None

                # Obtém o HTML da página
                html_content = driver.page_source

                # Codifica em bytes usando o BytesIO
                file_obj = io.BytesIO(html_content.encode())

                # Configura o caminho do local onde o arquivo será armazenado no s3
                file_path = f'pipeline/raw/{self.type.lower()}/{self.city}/{datetime.now().date()}/{filename_pattern}-{page}.html'

                # Upload do arquivo no s3
                self.s3.upload_fileobj(file_obj, self.bucket, file_path) # warning: esta variável deve ser substituída para receber da classe ou da invocação da função
                logger.info(f"Page {page} ingested and saved to {file_path}")

                # Move a janela até o rodapé
                driver.execute_script("window.scrollTo(0,9000)")

                # Aguarda uma janela de espera para evitar receber o mesmo conteúdo
                time.sleep(delay_seconds)

                # Encontra o botão de próxima página e o insere na variável next page como um elemento do selenium
                next_page = driver.find_element(By.XPATH, '//*[@id="js-site-main"]/div[2]/div[1]/section/div[2]/div[2]/div/ul/li[9]/button')

                # Insere o valor da próxima página na variável page
                page = int(next_page.get_attribute('data-page'))

                # Clica no botão
                next_page.click()
            except NoSuchElementException:
                logger.error("An Exception Occurred, refreshing the page")
                driver.refresh()
            except ValueError as e:
                # implementar esse catch pros casos onde o driver já tenha percorrido todas as páginas.
                pass
        return True

