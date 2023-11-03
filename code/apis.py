# Builtins
from datetime import datetime, date
import requests
import time
import re
import base64
from random import randint
from abc import ABC, abstractmethod, abstractproperty

# Bibliotecas Externas
import pandas as pd
import bs4 #BeautifulSoup - Lida com estruturas de dados html
from fake_useragent import UserAgent
# import html5lib

class ProxyConfig():
    """
    Representa um objeto de configuração de Servidor de Proxy.

    Atributos:
        host (str): a string host do serviço de proxy.
        port (int): a porta de acesso ao serviço.
        username (str): o nome de usuário para autenticação.
        password (str): a senha para autenticação.

    Methods:
        get_proxy_auth() -> requests.auth.HTTPProxyAuth:
            Retorna um objeto de autenticação de proxy da biblioteca Requestst.

    Properties:
        proxy_list (dict): Um dicionário com as strings completas de configuração de proxy para HTTP e HTTPS.

    Exemplo:
        ```Python
        # Chamada:
        proxy = ProxyConfig(
                        host=proxy_host,
                        port=proxy_port,
                        username=proxy_username,
                        password=proxy_password)
        # Retorno:
        <apis.ProxyConfig at 0x1407f9610>
        ```
    """
    def __init__(self, host:str, port:int, username:str, password:str):
        """
        Instancia um proxy para ser utilizado nos crawlers

        Artgs:
            - host (str): a string host do serviço de proxy.
            - port (int): a porta de acesso ao serviço.
            - username (str): o nome de usuário para autenticação.
            - password (str): a senha para autenticação.
        Retorna:

        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
    
    @property
    def proxy_list(self) -> dict:
        """
        Retorna um dicionário com strings para proxy HTTP e HTTPS
        """
        return {
            'http': f'http://{self.username}:{self.password}@{self.host}:{self.port}',
            'https': f'http://{self.username}:{self.password}@{self.host}:{self.port}'
                }

    def get_proxy_auth(self) -> requests.auth.HTTPProxyAuth:
        """
        Retorna um objeto do tipo requests.auth.HTTPProxyAuth utilizando o usernamee e senha do proxy.
        """
        return requests.auth.HTTPProxyAuth(self.username, self.password)
    


class ResultSet(pd.DataFrame):
    def __init__(self):
        super().__init__(
            data={'data': pd.Series(dtype='datetime64[ns]'),
                'fonte': pd.Series(dtype='str'),
                'descricao': pd.Series(dtype='str'),
                'endereco': pd.Series(dtype='str'),
                'rua': pd.Series(dtype='str'),
                'numero': pd.Series(dtype='int'),
                'bairro': pd.Series(dtype='str'),
                'cidade': pd.Series(dtype='str'),
                'valor': pd.Series(dtype='float'),
                'periodicidade': pd.Series(dtype='str'),
                'condominio': pd.Series(dtype='float'),
                'area': pd.Series(dtype='float'),
                'qtd_banheiros': pd.Series(dtype='int'),
                'qtd_quartos': pd.Series(dtype='int'),
                'qtd_vagas': pd.Series(dtype='int'),
                'url': pd.Series(dtype='str')
                    })

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

    def __init__(self, cidade:str, delay_seconds=0,proxy:ProxyConfig=None) -> None:
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
    
    @abstractproperty
    def _result_count(self) -> int:
        pass
        
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
    
    @abstractmethod
    def _get_endpoint(self) -> str:
        pass
    
    def _get_new_page_number(self, method='seq') -> int:
        """
        Obtém um novo número de página dentro do total de resultados.

        Args:
            method - 'seq' ou 'rand', seq varre as páginas uma a uma, enquanto 'rand' seleciona páginas
            aleatórias entre as possíveis para a quantidade de resultados.

        Retorna:
            Um inteiro representando o novo número de página.
        """
        if method == 'seq':
            return self.current_page + 1
        elif method == 'rand':
            return randint(1,round(self._result_count/self._results_per_page))
        else:
            raise ValueError('Erro: O parâmetro "method" aceita os valores "seq" | "rand"')

    
    def _extract_current_page(self, max_retries=5, backoff_factor=2) -> requests.models.Response:
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
            if retries > 0 or self.current_page > 1:
                time.sleep(self.delay_seconds)
            response = requests.get(
                url= self._get_endpoint(),
                proxies= self.proxy.proxy_list,
                headers={'User-Agent': UserAgent().random}
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

    @abstractmethod 
    def _extract_listings_from_soup(self, soup) -> bs4.element.ResultSet:
        pass

    @abstractmethod
    def extract_address(self, string) -> dict:
        pass
    
    def _extract_attribute(self, listing, tag, attr, default, post_process=None):
        """
        Extrai um atributo de um anúncio usando a tag e o atributo especificados.

        Args:
            listing: O anúncio do qual extrair o atributo.
            tag: A tag a ser procurada.
            attr: O atributo a ser recuperado.
            default: O valor padrão a ser retornado se o atributo não for encontrado.
            post_process: (opcional) Uma função para pós-processar o atributo extraído.

        Returns:
            O valor do atributo extraído, ou o valor padrão se o atributo não for encontrado.

        Raises:
            Exception: Se ocorrer um erro durante a extração do atributo.
        """
        try:
            result = listing.find(tag, attr)
            if post_process:
                result = post_process(result)
            return result
        except Exception as e:
            return e

    @abstractmethod
    def _format_listing(self, listing=None) -> list:
        pass
    
    def _append_formatted_listing(self, listing=None) -> None:
        url = listing[-1]
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
        except Exception:
            print(f'Error appending the following listing:\n{listing}, {Exception}')
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
        except Exception:
            print(f'Something went wrong while formating the listings of the page {self.current_page}: {Exception}')
        else:
            print(f'{added_listings} novos anúncios adicionados na página {self.current_page}')
            print(f'{formatted}')
            self.current_page = self._get_new_page_number()
    
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
                
    
    def extract_address(self, string) -> dict:
        """
        Extrai os componentes do endereço de uma string.

        Args:
            string: Uma string representando o endereço.

        Retorna:
            Um dicionário contendo os componentes do endereço extraídos (bairro, numero, rua).
        """
        string = ''.join([char if char not in "-,/;|." else ',' for char in string])
        term_list = string.split(',')[::-1]
        bairro = term_list[2] if len(term_list) > 2 else None
        numero = int(''.join(re.findall(r'\d', string))) if re.findall(r'\d', string) else None
        rua = term_list[0] if term_list else None
        return dict(
            bairro = bairro,
            numero = numero,
            rua = rua
        )

    
    def _get_endpoint(self) -> str:
        """
        Obtém o endpoint da API com base no endpoint base e a página atual.

        Retorna:
            Uma string representando o endpoint da API pronto para extração.
        """
        return f'{self.endpoint}{self.current_page}'

    
    def _format_listing(self, listing=None) -> list:
        """
        Formata as informações do anúncio.

        Args:
            listing: Um objeto BeautifulSoup representando as informações do anúncio.

        Retorna:
            Uma lista contendo as informações do anúncio formatadas.
        """
        data = datetime.now()
        fonte = self.type
        cidade = self.city
        formatted = []
        try:
            descricao = listing.find('span', {'class': 'js-card-title'}).text.strip()
        except TypeError:
            descricao = None
        try:
            endereco = listing.find('span', {'class': 'property-card__address'}).text.replace('-',',').replace('|','').strip()
        except TypeError:
            endereco = ''
        try:
            rua = self.extract_address(endereco)['rua']
        except TypeError:
            rua = None
        try:
            numero = self.extract_address(endereco)['numero']
        except TypeError:
            numero = None   
        try:
            bairro = self.extract_address(endereco)['bairro']
        except TypeError:
            bairro = None
        try:
            valor = listing.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('.','').split('/')[0]
        except TypeError:
            valor = None
        try:
            periodicidade = listing.find('div', {'class': 'property-card__price'}).text.replace('R$','').replace('.','').split('/')[1].split(' ')[0]
        except TypeError:
            periodicidade = None
        try:
            condominio = listing.find('strong', {'class': 'js-condo-price'}).text.replace('R$','').strip()
        except AttributeError:
            condominio = None
        try:
            area = listing.find('span', {'class': 'js-property-card-detail-area'}).text.strip()
        except TypeError:
            area = None
        try:
            qtd_banheiros = listing.find('li', {'class': 'property-card__detail-bathroom'}).text.strip()[0]
            qtd_banheiros = int(''.join(re.findall(r'\d', qtd_banheiros)))
        except ValueError:
            qtd_banheiros = None
        try:
            qtd_quartos = listing.find('li', {'class': 'property-card__detail-room'}).text.strip()[0]
            qtd_quartos = int(''.join(re.findall(r'\d', qtd_quartos)))
        except ValueError:
            qtd_quartos = None
        try:
            qtd_vagas = listing.find('li', {'class': 'property-card__detail-garage'}).text.strip()[0]
            qtd_vagas = int(''.join(re.findall(r'\d', qtd_vagas)))
        except ValueError:
            qtd_vagas = None
        try:
            link = 'https://vivareal.com.br' + listing.find('a', {'class': 'property-card__labels-container'})['href']
        except TypeError:
            link = None
        return (data,fonte,descricao,endereco,rua,numero,bairro,cidade,valor,periodicidade,condominio,area,qtd_banheiros,qtd_quartos,qtd_vagas,link)
# Classe GithubAPI: Gerencia a integração do notebook com o Github

class GithubApi():
    def __init__(self, token:str, owner:str, repo:str) -> None:
        """
        Inicia a classe GithubApi com o token de autenticação, usuário e repositório.
        
        Args:
        - token: string do token pessoal ou fine-grained.
        - owner: nome de usuário
        - repo: nome do repositório a ser conectado
        
        Retorna:
        Uma instância do objeto GithubApi
        """
        self.token = token
        self.owner = owner
        self.repo = repo
        self.base_url = f'https://api.github.com/repos/{owner}/{repo}'
        
    def get_url(self, file_path:str) -> str:
        """
        Retorna uma url formatada com os parâmetros da api do usuário
        
        Args:
        - file_path: O caminho para o arquivo dentro do repositório
        
        Retorna:
        URL no formato
        ```python
        api.get_url('file.csv')
        # https://api.github/.com/repos/cacau/florianopolis_rent_pricing_monitoring/file.csv
        ```
        """
        return f'{self.base_url}/contents/{file_path}'
        
    def get_file_info(self, file_url) -> str:
        """
        Recupera as informações de armazenamento de um arquivo de um repositório do Github
        
        Args:
        = file_url: url do arquivo obtida com a função get_url.
        
        Retorna:
        download_url: Url direta do conteúdo do arquivo.
        current_sha: chave criptografada com permissão de alterar o arquivo
        file_url: URL de local do arquivo fornecida pelo github
        """
        headers = {'Authorization': f'token {self.token}'}
        response = requests.get(url=file_url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        download_url = response_json['download_url']
        current_sha = response_json['sha']
        return download_url, current_sha
    
    def _download_current_content(self, download_url) -> pd.DataFrame:
        """
        Baixa o conteúdo do arquivo a partir da url de download.
        
        O Método self._download_current_content analisa a string da url de download para identificar
        o formato de arquivo e então utiliza um método de extração adequado para o tipo de formato.
        
        No momenro apenas o download de arquivos csv está implementado com a função pd.read_csv.
        
        Args:
        - download_url: url do arquivo obtida com a função get_file_info.
        
        Retorna:
        Dataframe Pandas com os valores do arquivo.
        """
        extension = download_url.split('.')[-1]
        if extension == 'csv':
            return pd.read_csv(download_url,index_col=0)
        elif extension == 'parquet':
            return pd.read_csv
        else:
            raise TypeError(f'file format {extension} is not supported')
                
    def _append_new_content(self,current_content=pd.DataFrame, new_content=pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona o conteúdo do novo result_set ao Dataframe existente.
        
        Kwargs:
        - current_content: Dataframe do dataset atual. 
        - new_content: Dataframe do novo result_set.
        
        Retorna:
        Dataframe Pandas com os valores unidos.
        """
        return pd.concat([current_content,new_content])
    
    def _get_encoded_content(self, appended_content=pd.DataFrame, file_format='csv'):
        """
        Formata o conteúdo do dataframe para inclusão no Github
        
        Kwargs:
        - appended_content: Dataframe com os valores a serem codificados para envio.
        - file_format: O formato para codificação, CSV por padrão.
        
        Retorna:
        Arquivo base64 na codificação desejada.
        """
        if file_format != 'csv':
            raise TypeError('Unsuported file type in _get_encoded_content method')
        csv_data = appended_content.to_csv()
        return base64.b64encode(csv_data.encode()).decode('utf-8')
    
    def _put_content(self, headers, data, url) -> requests.models.Response:
        """
        Realiza requisição PUT com payload formatado.
        
        Kwargs:
        - appended_content: Dataframe com os valores a serem configurados para envio.
        
        Retorna:
        Arquivo base64 na codificação desejada.
        """
        try:
            response = requests.put(url=url, headers=headers, json=data)
            response.raise_for_status()
            return response
        except requests.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        finally:
            if response.status_code == 200:
                print("File updated successfully.")
            else:
                print(f"Failed to update file. Status code: {response.status_code}")
    
    def update_file_content(self, file_path, new_content, method='append') -> None:
        """
        Realiza o update do conteúdo de um arquivo a partir de um path e um novo conteúdo.
        
        Args:
        - file_path: o caminho do arquivo dentro do repositório.
        - new_content: o Dataframe com o conteúdo a ser inserido.
        
        Kwargs:
        - method: 'append' Adiciona o novo conteúdo ao existente | 'overwrite' sobrescreve o conteúdo com o novo.
        """
        file_url = self.get_url(file_path=file_path)
        download_url, current_sha = self.get_file_info(file_url)
        current_content = self._download_current_content(download_url)
        if method == 'append':
            appended_content = self._append_new_content(current_content, new_content)
        elif method == 'overwrite':
            appended_content = new_content
        else:
            raise TypeError('"method" must be one of "append" or "overwrite".')
        encoded_content = self._get_encoded_content(appended_content)
        commit_message = "Automatically updated via Kaggle script"
        headers = {'Authorization': f'token {self.token}'}
        data = {"message": commit_message,"content": encoded_content,"sha": current_sha}
        self._put_content(headers=headers, data=data, url=file_url)
        