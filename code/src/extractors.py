# Builtins
from datetime import datetime
import time
import re
import io

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
from selenium import webdriver
from selenium.webdriver.common.by import By
import pyarrow.parquet as pq
import pyarrow as pa
import boto3

# Módulos Personalizados
from utils import ResultSet

class Extractor():

    def __init__(self, cidade:str, webdriver:webdriver = None, s3:boto3.client = None) -> None:
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
        self.s3 = s3
        self.type = 'Vivareal'
        
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
    
    def parse_html(self, html=None) -> bs4.BeautifulSoup:
        """
        Transforma o conteúdo html da response em um objeto bs4.BeautifulSoup.

        Args:
            response: Um objeto requests.models.Response representando a resposta HTTP a ser transformada.

        Retorna:
            Um objeto BeautifulSoup representando a resposta HTML analisada.
        """
        return bs4.BeautifulSoup(html, features="html5lib")
    
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
        
    
    def process_file(self, file) -> None:
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
            soup = self.parse_html(html=file)
            listings = self.extract_listings_from_soup(soup=soup)
            added_listings = 0
            for i in listings:
                formatted = self.format_listing(listing = i)
                success = self.append_formatted_listing(listing=formatted)
                if success == True:
                    added_listings += 1
        except Exception as e:
            print(f'Something went wrong while formating the listings of the current file: {e}')
        else:
            print(f'{added_listings} new listings added to result set')
            

    def process_folder(self, bucket_name: str, folder_path: str, filename_pattern:str, output_format: str = None, max_pages : int = None):
        folder_name = folder_path.split('/')[4]
        if output_format is None:
            output_format = ['csv','parquet']
        if output_format not in ['csv', 'parquet']:
            raise ValueError("Output Format must be one of 'csv', 'parquet'")
            
        s3objects = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        pages = 1
        for obj in s3objects.get('Contents', []):
            file_name = obj['Key']
            if file_name.endswith('.html'):
                response = self.s3.get_object(Bucket=bucket_name, Key=file_name)
                html_content = response['Body'].read().decode('utf-8')
                self.process_file(html_content)
                pages += 1
            if max_pages and (pages > max_pages):
                break
        file_path = f'pipeline/processed/{self.type.lower()}/{self.city}/{filename_pattern}-{folder_name}.{output_format}'
        table = pa.Table.from_pandas(self.result_set)
        output_buffer = io.BytesIO()
        pq.write_table(table, output_buffer)
        output_buffer.seek(0)
        self.s3.upload_fileobj(output_buffer, bucket_name, file_path)

    @property
    def endpoint(self) -> str:
        """
        Obtém o endpoint da API.

        Retorna:
            Uma string representando o endpoint base da API.
        """
        return f'https://www.vivareal.com.br/aluguel/santa-catarina/{self.city}/'
    

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
            'street': lambda x: x.find('span', {'class': 'property-card__address'}).text.strip().split('-')[::-1][2].split(',')[0],
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