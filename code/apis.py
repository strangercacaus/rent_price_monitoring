# Builtins
from datetime import datetime, date
import requests
import time
import re
from random import randint
from abc import ABC, abstractmethod, abstractproperty

# Bibliotecas Externas
import bs4 #BeautifulSoup - Lida com estruturas de dados html

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

    def __init__(self, cidade:str, delay_seconds:int=0, proxy:ProxyConfig=None) -> None:
        """
        Instancia um objeto da classe VivaRealApi.

        O objeto VivaRealApi reune as funções de scrolling, extração e formatação dos resultados do site Viva Real,
        sendo o principal componente do web_scraping realizado no projeto

        Args:
            cidade: Uma string representando a cidade a ser monitorada.
            delay_seconds: Opcional, um número inteiro representando o atraso em segundos entre as requisições sequenciais.
        """
        self.current_page = 1
        self.city = cidade
        self.delay_seconds = delay_seconds
        self._last_http_response = None
        self.result_set = ResultSet()
        self.proxy = proxy

    @property
    @abstractmethod
    def type(self) -> str:
        pass
        
    @property
    @abstractmethod
    def endpoint(self) -> str:
        pass

    @property
    def _first_page(self) -> bs4.BeautifulSoup:
        """
        Obtém a resposta HTML da primeira página e a processa no format bs4.BeautifulSoup.

        Retorna:
            Um objeto BeautifulSoup representando a resposta HTML analisada.
        """
        response = self._extract_current_page()
        return self._parse_html_response(response=response)
    
    @property
    def _results_per_page(self) -> int:
        """
        Obtém o número de resultados por página a partir do total de resultados e número de resultados da primeira página.

        Retorna:
            Um número inteiro representando o número de resultados por página.
        """
        soup = self._first_page
        listings = self._extract_listings_from_soup(soup=soup)
        return len(listings)
    
    
    @property
    @abstractmethod
    def _result_count(self) -> int:
        pass
    
    
    @abstractmethod
    def _get_endpoint(self) -> str:
        pass
    
    @abstractmethod
    def _get_new_page_number(self, page:bs4.BeautifulSoup, method='seq') -> int:
        """
        Obtém um novo número de página dentro do total de resultados.

        Args:
            method - 'seq' ou 'rand', seq varre as páginas uma a uma, enquanto 'rand' seleciona páginas
            aleatórias entre as possíveis para a quantidade de resultados.

        Retorna:
            Um inteiro representando o novo número de página.
        """
        pass

    @abstractmethod 
    def _extract_listings_from_soup(self, soup) -> bs4.element.ResultSet:
        pass

    @abstractmethod
    def load_extractor(self, value_id:str) -> callable:
        pass
    
    def extract_value(self, listing, value_id):
        try:
            func = self.load_extractor(value_id)
            return func(listing)
        except (AttributeError, TypeError, ValueError):
            return None
        except Exception as e:
            print(f'{__class__}.{__name__} Exception: {e}')
    
    def _format_listing(self, listing=None) -> dict:
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
            qtd_quartos = self.extract_value(value_id='rooms', listing=listing),
            qtd_vagas = self.extract_value(value_id='parkinspaces', listing=listing),
            url = self.extract_value(value_id='url', listing=listing),
            amenities = self.extract_value(value_id='amenities', listing=listing)
        )

    def _extract_current_page(self, max_retries=6, backoff_factor=4) -> requests.models.Response:
        """
        Extrai a página atual da API e atualiza a propriedade .last_http_response.

        Args:
            max_retries: Um inteiro representando o número máximo de tentativas em caso de erro HTTP.
            backoff_factor: Um inteiro representando o fator de espera exponencial.

        Retorna:
            Um objeto requests.models.Response representando a resposta HTTP.
        """
        retries = 0
        while retries < max_retries:
            url = self._get_endpoint()
            print(f'Getting page at {url}')
            if retries > 0 or self.current_page > 1:
                time.sleep(self.delay_seconds)
            response = requests.get(
                url= url,
                proxies = self.proxy.proxy_list if self.proxy is not None else None
                )
            
            self._last_http_response = response.status_code
            
            if response.status_code < 300:  # Sucesso
                return response
            
            elif response.status_code == 429:  # Too many requests
                print(f"Rate limited. Retrying in {backoff_factor ** retries} seconds.")
                time.sleep(backoff_factor ** retries)
                retries += 1
                
            else:
                print(f"Request failed with status code {response.status_code}")
                return None

    
    def _parse_html_response(self, response=None) -> bs4.BeautifulSoup:
        """
        Transforma o conteúdo html da response em um objeto bs4.BeautifulSoup.

        Args:
            response: Um objeto requests.models.Response representando a resposta HTTP a ser transformada.

        Retorna:
            Um objeto BeautifulSoup representando a resposta HTML analisada.
        """
        if response and response.status_code < 300:
            return bs4.BeautifulSoup(response.text, features="html5lib")
        elif response:
            print(f'No valid response to be parsed. Status code {response.status_code}.')
            return None
        else:
            print('Empty request')

    
    def _append_formatted_listing(self, listing:dict=None) -> None:
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
    
    def _ingest_current_page(self) -> None:
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
            print(f'Current_page: {self.current_page}')
            response = self._extract_current_page()
            soup = self._parse_html_response(response=response)
            listings = self._extract_listings_from_soup(soup=soup)
            added_listings = 0
            for i in listings:
                formatted = self._format_listing(listing = i)
                success = self._append_formatted_listing(listing=formatted)
                if success == True:
                    added_listings += 1
        except Exception as e:
            print(f'Something went wrong while formating the listings of the page {self.current_page}: {e}')
        else:
            print(f'{added_listings} novos anúncios adicionados na página {self.current_page}')
            print(f'{formatted}')
            self.current_page = self._get_new_page_number(page=soup)
    
    def ingest_listings(self, all=True, max_attempts=None) -> None:
        """
        Executa uma rotina de ingestão com base nos argumentos fornecidos.

        Args:
            all: Um booleano indicando se todos os anúncios devem ser ingeridos ou não, por padrão True.
            max_attempts: Um inteiro representando o número máximo de tentativas, por padrão None.

        Retorna:
            None.
        """
        attempts = 0
        if all == True:
            while self.result_set.shape[0] < self._result_count:
                    self._ingest_current_page()
                    attempts += 1
                    print(f'Current page: {self.current_page}')
        elif max_attempts:
            if type(max_attempts) == int and max_attempts > 0:
                self.current_page = 1
                while attempts <= max_attempts:
                    self._ingest_current_page()
                    attempts += 1
            else:
                raise TypeError('pages_number: This parameter only accepts numbers above zero.')

class VivaRealApi(ListingAPI):
    def __init__(self,cidade:str,delay_seconds:int=0, proxy:ProxyConfig=None):
        super().__init__(cidade=cidade,delay_seconds=delay_seconds,proxy=proxy)

    @property
    def type(self) -> str:
        return 'Viva Real'

    @property
    def endpoint(self) -> str:
        """
        Obtém o endpoint da API.

        Retorna:
            Uma string representando o endpoint base da API.
        """
        return f'https://www.vivareal.com.br/aluguel/santa-catarina/{self.city}/?pagina='
    
    @property
    def _result_count(self) -> int:
        """
        Obtém o número total de resultados divulgados pelo portal para a cidade atribuída.
        
        O valor é obtido a partir da identificação de um elemento 'Strong' da classe 'results-summary__count'

        Retorna:
            Um número inteiro representando a quantidade total de resultados fornecida pelo portal.
        """
        soup = self._first_page
        try:
            return int(soup.find('strong',{'class':'results-summary__count'}).text.replace('.',''))
        except Exception:
            print(f'No houses were found for the given page.\n the HTML structure of the page might have been altered...\n{Exception}')
            return 0
        
    def _get_new_page_number(self, page:bs4.BeautifulSoup, method='seq') -> int:
        """
        Obtém um novo número de página dentro do total de resultados.

        Args:
            method - 'seq' ou 'rand', seq varre as páginas uma a uma, enquanto 'rand' seleciona páginas
            aleatórias entre as possíveis para a quantidade de resultados.

        Retorna:
            Um inteiro representando o novo número de página.
        """
        if method == 'seq':
            return int(page.find('button',{"class": 'js-change-page', "title": "Próxima página"})['data-page'])
        elif method == 'rand':
            return randint(1,round(self._result_count/self._results_per_page))
        else:
            raise ValueError('Erro: O parâmetro "method" aceita os valores "seq" | "rand"')
    
    def _extract_listings_from_soup(self, soup) -> bs4.element.ResultSet:
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

    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint da API com base no endpoint base e a página atual.

        Retorna:
            Uma string representando o endpoint da API pronto para extração.
        """
        return f'{self.endpoint}{self.current_page}'

    def load_extractor(self, value_id: str) -> callable:
        cases = {
            'id': lambda x: x.find('a', {'class': 'property-card__content-link js-card-title'})['href'].split('-')[-1],
            'url': lambda x: 'https://vivareal.com.br' + x.find('a', {'class': 'property-card__content-link js-card-title'})['href'],
            'address': lambda x: x.find('span', {'class': 'property-card__address'}).text.strip(),
            'street': lambda x: next((char for char in x.find('span', {'class': 'property-card__address'}).text.strip().replace('-', ',').replace('/', ',').replace(';', ',').replace('|', ',').split(',')[::-1] if char), None),
            'number': lambda x: int(''.join(char for char in x.find('span', {'class': 'property-card__address'}).text.strip() if char.isdigit())) or None,
            'neighborhood': lambda x: x.find('span', {'class': 'property-card__address'}).text.strip().replace('-', ',').replace('/', ',').replace(';', ',').replace('|', ',').split(',')[::-1][2] if len(x.find('span', {'class': 'js-card-title'}).text.strip().replace('-', ',').replace('/', ',').replace(';', ',').replace('|', ',').split(',')[::-1]) > 2 else None,
            'rooms': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-room'}).text.strip()[0]))),
            'bathrooms': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-bathroom'}).text.strip()[0]))),
            'parkingspaces': lambda x: int(''.join(re.findall(r'\d', x.find('li', {'class': 'property-card__detail-garage'}).text.strip()[0]))),
            'periodicity': lambda x: x.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('.','').split('/')[1].split(' ')[0],
            'title': lambda x: x.find('span', {'class': 'js-card-title'}).text.strip(),
            'area': lambda x: int(''.join(re.findall(r'\d', x.find('span', {'class': 'js-property-card-detail-area'}).text.strip()))),
            'price': lambda x: x.find('div', {'class': 'property-card__price'}).text.replace('R$', '').replace('.', '').split('/')[0],
            'condoprice': lambda x: x.find('strong', {'class': 'js-condo-price'}).replace('R$', '').strip(),
            'amenities': lambda x: '; '.join(tag.text.strip() for tag in x.find_all('li', {'class': 'amenities__item'}))
        }
        return cases.get(value_id)