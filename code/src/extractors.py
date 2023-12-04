# Builtins
from datetime import datetime
import time
import re
import io
import logging
from io import BytesIO

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html
import pyarrow.parquet as pq
import pyarrow as pa
import boto3
import pandas as pd
import numpy as np
# Módulos Personalizados
from utils import ResultSet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Add a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class Extractor():

    def __init__(self, cidade:str, s3:boto3.client = None) -> None:
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
        self.additions_count = 0
    
    def extract_value(self, listing, value_id):
        try:
            func = self.load_extractor(value_id)
            return func(listing)
        except (AttributeError, TypeError, ValueError, IndexError):
            return None
        except Exception as e:
            logger.info(f'{__name__} Exception: {e}')
    
    def format_listing(self, listing=None) -> dict:
        return dict(
            data = datetime.now(),
            fonte = self.type,
            id = self.extract_value(value_id='id', listing=listing),
            descricao = self.extract_value(value_id='title', listing=listing),
            tipo = self.extract_value(value_id='type',listing=listing),
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
            self.result_set.loc[self.additions_count] = listing
            self.additions_count += 1
        except Exception as e:
            logger.info(f'Error appending the following listing:\n{listing}, {e}')
        
    
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
            added_listings = set()
            for i in listings:
                listing_id = self.extract_value(value_id='id', listing=i)
                if listing_id not in added_listings:
                    formatted = self.format_listing(listing = i)
                    self.append_formatted_listing(listing=formatted)
                    added_listings.add(listing_id)
        except Exception as e:
            logger.info(f'Something went wrong while processing the file: {e}')
        else:
            logger.info(f'{len(added_listings)} new listings added to result set')
            

    def process_folder(self, bucket_name: str, folder_path: str, filename_pattern:str, output_format: str = None, max_pages : int = None):
        self.additions_count = 0
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
        file_path = f'pipeline/processed/{self.type.lower()}/{self.city}/extracted/{filename_pattern}-{folder_name}.{output_format}'
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
            'type': lambda x: x.find('span', {'class': 'js-card-title'}).text.strip().split('|')[0],
            'area': lambda x: int(''.join(re.findall(r'\d', x.find('span', {'class': 'js-property-card-detail-area'}).text.strip()))),
            'price': lambda x: int(''.join(re.findall(r'\d',x.find('div', {'class': 'property-card__price'}).text))),
            'condoprice': lambda x: int(''.join(re.findall(r'\d', x.find('strong', {'class': 'js-condo-price'}).text.replace('R$ ', '')))),
            'amenities': lambda x: '; '.join(tag.text.strip() for tag in x.find_all('li', {'class': 'amenities__item'}))
        }
        return cases.get(value_id)
    
class Formatter():
    def __init__(self, s3:boto3.client = None) -> None:
        self.s3 = s3
        self.type = 'vivareal' # variável fixada momentaneamente, no futuro alterar para ser passada na instanciação da classe
        self.city = 'florianopolis' # variável fixada momentaneamente, no futuro alterar para ser passada na instanciação da classe
    
    def format_df(self, dataframe=pd.DataFrame) -> pd.DataFrame:
        df = dataframe
        commercial_values = ['loja', 'ponto', 'box', 'conjunto', 'comercial', 'galpão', 'prédio', 'edifício', 'terreno']
        try:
            df['condominio'] = df['condominio'].fillna(0)
            df['categoria'] = np.where(df['tipo'].isin(commercial_values), 'Comercial', 'Residencial')
            df['valor_total'] = pd.to_numeric(df.apply(lambda row: row['valor'] + row['condominio'] if not pd.isnull(row['valor']) and not pd.isnull(row['condominio']) else row['valor'], axis=1).fillna(0))
            df['valor_m2'] = (df['valor'] / df['area']) / 30 if df['periodicidade'].str == 'Mês' else df['valor'] / df['area']
            df['valor_condo_m2'] = (df['condominio'] / df['area']) / 30 if df['periodicidade'].str == 'Mês' else df['condominio'] / df['area']
            formatted_df = df[(df['valor_m2'] < 500) &
                              (df['valor_m2'] >= 1) &
                              (df['area'] <= 2000) &
                              (df['valor_condo_m2'] <= 40) &
                              ((df['periodicidade'] == 'Dia')|(df['periodicidade'] == 'Mês'))]
        except Exception as e:
            logger.info(f'Error formatting file: {e}')
        finally:
            return formatted_df
    
    def parse_parquet_response(self, s3object=None) -> pd.DataFrame:
        obj = s3object['Body'].read()
        buffer = BytesIO(obj)
        parquet_table = pq.read_table(buffer)
        return parquet_table.to_pandas()
    
    def process_date(self, bucket_name: str, datestr: str):
        file_name = f'pipeline/processed/{self.type.lower()}/{self.city}/extracted/processed-{datestr}.parquet'
        obj = self.s3.get_object(Bucket=bucket_name, Key=file_name)
        if not file_name.endswith('.parquet'):
            raise ValueError("Invalid file format")
        df = self.parse_parquet_response(s3object=obj)
        formatted_df = self.format_df(dataframe=df)
        file_path = f'pipeline/processed/{self.type.lower()}/{self.city}/formatted/formatted-{datestr}.parquet'
        table = pa.Table.from_pandas(formatted_df)
        output_buffer = io.BytesIO()
        pq.write_table(table, output_buffer)
        output_buffer.seek(0)
        self.s3.upload_fileobj(output_buffer, bucket_name, file_path)
        
    def run(self, datestr=str, reprocess=False, bucket_name=str):
        # sourcery skip: remove-pass-body
        if reprocess == True:
            pass #implementar aqui caso para reprocessar toda a pasta de arquivos.
        else:
            self.process_date(datestr=datestr, bucket_name=bucket_name)