
#%%
from selenium import webdriver
from datetime import datetime, date
from apis import VivaRealApi
from utils import ResultSet, ProxyConfig, GithubApi
#%%

driver = webdriver.Chrome() #Instanciando Driver

viva = VivaRealApi(webdriver=driver, cidade='florianopolis') # Instanciando API
# %%
path = f'../data/raw/{viva.type}/{viva.city}/{datetime.now().date()}/' #Definindo caminho do dump na camada raw

# Chamando a função de ingestão
viva.ingest_pages(
    output_path=path,
    filename_pattern='page-',
    delay_seconds=2,
    all=False,
    pages=10)
# %%
