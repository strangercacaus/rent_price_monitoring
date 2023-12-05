import pandas as pd
import requests
import base64
import logging
from io import BytesIO
    
import boto3
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Add a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class ResultSet(pd.DataFrame):
    def __init__(self):
        super().__init__(
            data={
                'data': pd.Series(dtype='datetime64[ns]'),
                'fonte': pd.Series(dtype='str'),
                'id': pd.Series(dtype='int64'),
                'descricao': pd.Series(dtype='str'),
                'tipo': pd.Series(dtype='str'),
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
                'url': pd.Series(dtype='str'),
                'amenities': pd.Series(dtype='str')
                    })

class GithubApi():
    def __init__(self, token:str, owner:str, repo:str, branch:str) -> None:
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
        self.branch = branch
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
            logger.info(f'HTTP error occurred: {http_err}')
        except Exception as err:
            logger.info(f'Other error occurred: {err}')
        finally:
            if response.status_code == 200:
                logger.info("File updated successfully.")
            else:
                logger.info(response.content)
                logger.info(f"Failed to update file. Status code: {response.status_code}")
    
    def update_file_content(self, file_path:str, new_content:pd.DataFrame, method='overwrite') -> None:
        """
        Realiza o update do conteúdo de um arquivo a partir de um path e um novo conteúdo.
        
        Args:
        - file_path: o caminho do arquivo dentro do repositório.
        - new_content: o Dataframe com o conteúdo a ser inserido.
        
        Kwargs:
        - method: 'append' Adiciona o novo conteúdo ao existente | 'overwrite' sobrescreve o conteúdo com o novo.
        """
        file_url = self.get_url(file_path=file_path)
        if self.branch:
            file_url = f'{file_url}?ref={self.branch}'
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
        data = {"message": commit_message,
                "content": encoded_content,
                "sha": current_sha,
                "branch":self.branch}
        self._put_content(headers=headers, data=data, url=file_url)
        
class Aggregator():
    def __init__(self, s3):
        self.s3 = s3
        self.city = 'florianopolis'
        self.type = 'vivareal'

    def combine_parquet_files(self, bucket_name, prefix):
        s3 = self.s3
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        tables = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                data = file_obj['Body'].read()
                buffer = BytesIO(data)
                table = pq.read_table(buffer)
                tables.append(table)
        return pa.concat_tables(tables)

    def upload_combined_file(self, bucket_name, combined_table, s3_key):
        s3 = self.s3
        output_buffer = BytesIO()
        pq.write_table(combined_table, output_buffer)
        output_buffer.seek(0)
        self.s3.upload_fileobj(output_buffer, bucket_name, s3_key)

    def run(self, bucket_name, export_method:str='s3'):
        output_filename = f'pipeline/processed/{self.type.lower()}/{self.city.lower()}/curated/listings_history.parquet'
        prefix = f'pipeline/processed/{self.type.lower()}/{self.city}/formatted/'
        combined_table = self.combine_parquet_files(bucket_name, prefix)
        if export_method == 's3':
            self.upload_combined_file(bucket_name, combined_table, s3_key=output_filename)
            return True
        
        elif export_method == 'df':
            return combined_table.to_pandas()
        else:
            raise TypeError('Invalid export method, must be one of "s3", "df"')