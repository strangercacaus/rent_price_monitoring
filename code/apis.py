# Builtins
from datetime import datetime, date
import requests
import time
import re
import io
from random import randint
from abc import ABC, abstractmethod, abstractproperty

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
import boto3

# Módulos Personalizados
from utils import ProxyConfig, ResultSet

class ListingAPI(ABC):
    """
    Uma classe abstrata que representa um portal de anúncios a ser implementado.

    Args:
        cidade: Uma string representando a cidade que será monitorada, sem caracteres especiais ou acentuação.
        delay_seconds: Opcional - Um valor inteiro para definir o atraso em segundos entre as requisições (0 por padrão).

    Atributos:
        type: Uma str representando o tipo de API (Viva Real).
        current_page: Um int representando o número da página atual.
        city: Uma str representando a cidade da api instanciada.
        delay_seconds: Um inteiro representando o atraso em segundos entre as requisições sequenciais.
        _last_http_response: O status_code da última resposta HTTP recebida.
        result_set: Um DataFrame do pandas representando o conjunto de resultados da sessão ativa de extração de dados.

    Métodos:
        endpoint: Uma propriedade que retorna o endpoint base da API.
        _result_count: Uma propriedade que retorna o número total de resultados exibidos pelo portal.
        _results_per_page: Uma propriedade que retorna o número total de resultados encontrados na primeira página.
        _first_page: Um método que retorna a resposta HTML analisada em formato bs4.Soup da primeira página.
        _get_endpoint: Um método interno que retorna um endpoint da API com base na cidade, página e em uma seed randômica.
        _get_new_page_number: Um método interno que retorna um número de página aleatório com base no tamanho do universo de resultados.
        _extract_current_page: Um método interno que extrai o html da página atual da API.
        _parse_html_response: Um método interno que converte o html em um objeto bs4.Soup.
        _extract_attribute()L Uma função auxiliar da extract_listings responsável por executar a lógica e gerenciar erros.
        _extract_listings_from_soup: Um método interno que extrai os objetos da classe 'article' do objeto bs4.Soup obtido.
        _format_listing: Um método que formata uma listagem para inserção no result_set da instância.
        _append_formatted_listing: Um método interno que adiciona uma listagem formatada ao conjunto de resultados.
        _ingest_current_page: Um método interno que ingere a página atual de listagens.
        ingest_listings: Um método que ingere todas as listagens ou um número especificado de páginas com base nos parâmetros fornecidos.
        dump_result_set: Um método que salva o conjunto de resultados em um arquivo csv.

    Raises:
        TypeError: Se no método ingest_listings o parâmetro max_attempts não for um inteiro positivo.

    Exemplo:
        # Criar uma instância de VivaRealApi
        api = VivaRealApi(cidade='Florianopolis', delay_seconds=2)

        # Ingerir todas as listagens
        api.ingest_listings(all=True)

        # Salvar o conjunto de resultados em um arquivo CSV
        api.dump_result_set(path='/caminho/para/salvar/', format='csv')
    """

    def __init__(self, cidade:str,  webdriver:webdriver, s3:boto3.client = None, proxy:ProxyConfig=None) -> None:
        """
        Instancia um objeto da classe VivaRealApi.

        O objeto VivaRealApi reune as funções de scrolling, extração e formatação dos resultados do site Viva Real,
        sendo o principal componente do web_scraping realizado no projeto

        Args:
            cidade: Uma string representando a cidade a ser monitorada.
            delay_seconds: Opcional, um número inteiro representando o atraso em segundos entre as requisições sequenciais.
        """
        self.city = cidade
        self.result_set = ResultSet()
        self.proxy = proxy
        self.s3 = s3
        self.webdriver = webdriver
    @property
    @abstractmethod
    def type(self) -> str:
        pass
        
    @property
    @abstractmethod
    def endpoint(self) -> str:
        pass

    @abstractmethod 
    def extract_listings_from_soup(self, soup) -> bs4.element.ResultSet:
        pass

    @abstractmethod
    def load_extractor(self, value_id:str) -> callable:
        pass
    
    @abstractmethod
    def ingest_pages(self, filename_pattern:str, all:bool=True, pages:int=None, delay_seconds:int=0) -> None:
        """
        Executa uma rotina de ingestão com base nos argumentos fornecidos.

        Args:
            all: Um booleano indicando se todos os anúncios devem ser ingeridos ou não, por padrão True.
            max_attempts: Um inteiro representando o número máximo de tentativas, por padrão None.

        Retorna:
            None.
        """
        pass
    
    def extract_value(self, listing, value_id):
        try:
            func = self.load_extractor(value_id)
            return func(listing)
        except (AttributeError, TypeError, ValueError):
            return None
        except Exception as e:
            print(f'{__class__}.{__name__} Exception: {e}')
    
    def format_listing(self, listing=None) -> dict:
        return dict(
            data = datetime.now(),
            fonte = self.type,
            id = self.extract_value(value_id='id', listing=listing),
            descricao = self.extract_value(value_id='title', listing=listing),
            endereco = self.extract_value(value_id='address', listing=listing),
            rua = self.extract_value(value_id='street', listing=listing),
            numero = self.extract_value(value_id='number', listing=listing),
            bairro = self.extract_value(value_id='neighborhood', listing=listing),
            cidade = self.city,
            valor = self.extract_value(value_id='price', listing=listing),
            periodicidade = self.extract_value(value_id='periodicity', listing=listing),
            condominio = self.extract_value(value_id='condoprice', listing=listing),
            area = self.extract_value(value_id='area', listing=listing),
            qtd_banheiros = self.extract_value(value_id='bathrooms', listing=listing),
            qtd_quartos = self.extract_value(value_id='rooms', listing=listing),
            qtd_vagas = self.extract_value(value_id='parkingspaces', listing=listing),
            url = self.extract_value(value_id='url', listing=listing),
            amenities = self.extract_value(value_id='amenities', listing=listing)
        )
    
    def parse_html(self, file=None) -> bs4.BeautifulSoup:
        """
        Transforma o conteúdo html da response em um objeto bs4.BeautifulSoup.

        Args:
            response: Um objeto requests.models.Response representando a resposta HTTP a ser transformada.

        Retorna:
            Um objeto BeautifulSoup representando a resposta HTML analisada.
        """
        return bs4.BeautifulSoup(file, features="html5lib")
    
    def append_formatted_listing(self, listing:dict=None) -> None:
        url = listing.get('url')
        """
        Adiciona o anúncio formatado ao conjunto de resultados.

        Args:
            listing: Uma lista contendo as informações formatadas do anúncio.

        Retorna:
            None
        """
        try:
            if url not in self.result_set['url'].to_list():
                self.result_set.loc[self.result_set.shape[0]] = listing
                return True
            else:
                print(f'{url} Already exists in the result_set')
        except Exception as e:
            print(f'Error appending the following listing:\n{listing}, {e}')
            return False
        else:
            return False
    
    def process_current_page(self, page) -> None:
        """
        Ingere a página atual de anúncios utilizando os métodos internos da API.

        O método _ingest_current_page realiza uma chamada para o método _extract_current_page(),
        transforma o retorno em um objeto bs4.BeautifulSoup utilizando o método _parse_html_response e então
        extrai as listagens utilizando o método _extract_listings_from_soup.

        Uma vez extraídas as listagens, define uma variável de controle 'added_listings' com valor 0 e percorre
        o resultset de listagens com o método _format_listing. Caso a formatação seja bem sucedida realiza uma chamada
        para o método _append_formatted_listing e incrementa a variável de controle no caso de uma inclusão resposta bem sucedida.

        ao final, realiza uma chamada para o método _get_new_page_number para preparar a próxima página para ingestão.

        Retorna:
            None.
        """
        try:
            soup = self.parse_html(response=page)
            listings = self.extract_listings_from_soup(soup=soup)
            added_listings = 0
            for i in listings:
                formatted = self.format_listing(listing = i)
                success = self.append_formatted_listing(listing=formatted)
                if success == True:
                    added_listings += 1
        except Exception as e:
            print(f'Something went wrong while formating the listings of the page {self.current_page}: {e}')
        else:
            print(f'{added_listings} novos anúncios adicionados na página {self.current_page}')
            print(f'{formatted}')
            self.current_page = self._get_new_page_number(page=soup)

class VivaRealApi(ListingAPI):
    def __init__(self, cidade:str,  webdriver:webdriver, s3:boto3.client = None, proxy:ProxyConfig=None):
        super().__init__(cidade=cidade, webdriver=webdriver, s3=s3, proxy=proxy)

    @property
    def type(self) -> str:
        return 'Vivareal'

    @property
    def endpoint(self) -> str:
        """
        Obtém o endpoint da API.

        Retorna:
            Uma string representando o endpoint base da API.
        """
        return f'https://www.vivareal.com.br/aluguel/santa-catarina/{self.city}/'
    
    
    def ingest_pages(self, filename_pattern:str, all:bool=True, pages:int=None, delay_seconds:int=0) -> None:
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
        while all or (pages is not None and page < pages):
            try:
                html_content = driver.page_source
                file_obj = io.BytesIO(html_content.encode())
                s3Client.upload_fileobj(file_obj, 'floriparentpricing', f'raw/{self.type.lower()}/{self.city}/{datetime.now().date()}/{filename_pattern}-{page}.html')
                driver.execute_script("window.scrollTo(0,9800)")
                driver.find_element(By.CSS_SELECTOR, ".pagination__item:nth-child(9) > .js-change-page").click()
                time.sleep(delay_seconds)
                page += 1
            except Exception as e:
                print(f': An Exception Occurred: {e}')
                break
        return True

    def extract_listings_from_soup(self, soup) -> bs4.element.ResultSet:
        """
        Extrai as listagens de anúncios do objeto bs4.BeautifulSoup.

        Utiliza o método soup.find_all para encontrar objetos html do tipo 'article' e classe 'property-card__container',
        retornando um objeto ResultSet com as correspondências

        Args:
            soup: Um objeto BeautifulSoup representando a resposta HTML analisada.

        Retorna:
            Um objeto ResultSet contendo as listagens extraídas.
        """
        return soup.find_all('article', {'class': 'property-card__container js-property-card'})

    def load_extractor(self, value_id: str) -> callable:
        cases = {
            'id': lambda x: int(''.join(re.findall(r'\d', x.find('a', {'class': 'property-card__content-link js-card-title'})['href'].split('-')[-1]))),
            'url': lambda x: 'https://vivareal.com.br' + x.find('a', {'class': 'property-card__content-link js-card-title'})['href'],
            'address': lambda x: x.find('span', {'class': 'property-card__address'}).text.strip(),
            'street': lambda x:  x.find('span', {'class': 'property-card__address'}).text.strip().split('-')[::-1][2].split(',')[0],
            'number': lambda x: int(''.join(char for char in x.find('span', {'class': 'property-card__address'}).text.strip() if char.isdigit())) or None,
            'neighborhood': lambda x: x.find('span', {'class': 'property-card__address'}).text.strip().replace('-',',').split(',')[-3],
            'rooms': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-room'}).text.strip()[0]))),
            'bathrooms': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-bathroom'}).find('span',{'class':'property-card__detail-value'}).text.strip()[0]))),
            'parkingspaces': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-garage'}).find('span',{'class':'property-card__detail-value'}).text.strip()[0]))),
            'periodicity': lambda x: x.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('\n','').replace('.','').split('/')[1].split(' ')[0],
            'title': lambda x: x.find('span', {'class': 'js-card-title'}).text.strip(),
            'area': lambda x: int(''.join(re.findall(r'\d', x.find('span', {'class': 'js-property-card-detail-area'}).text.strip()))),
            'price': lambda x: int(''.join(re.findall(r'\d',x.find('div', {'class': 'property-card__price'}).text))),
            'condoprice': lambda x: int(''.join(re.findall(r'\d', x.find('strong', {'class': 'js-condo-price'}).text.replace('R$ ', '')))),
            'amenities': lambda x: '; '.join(tag.text.strip() for tag in x.find_all('li', {'class': 'amenities__item'}))
        }
        return cases.get(value_id)