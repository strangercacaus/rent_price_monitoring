# Florianópolis Rent Price Monitoring

### Objetivo:

O objetivo deste projeto é capturar recorrentemente dados dos preços de aluguéis ativos em Florianópolis-SC e disponibilizar para analists de dados e interessados da região.

### Inspiração:

A ideia inicial do projeto veio de um exercício de ingestão de dados do bootcamp de engenharia de dados da How Education em 2023 que consistia em realizar o crawling do site vivareal e recuperar informações referentes aos imóveis.

Ao longo do tempo percebi que não havia uma base de dados pública que tivesse dados históricos dos valores de imóveis anunciados nos portais e por este motivo o objetivo deste projeto é expandir o estudo inicial realizando esta captura de dados de modo recorrente e abertamente disponível.

### Escopo:

O projeto se baseia na realização de uma rotina semanal de web-scraping utilizando a plataforma Kaggle:

[![Notebook](https://img.shields.io/badge/Acesse_o_Notebook-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/code/caueausec/florian-polis-rent-pricing-dataset-web-scraping)

Os dados são coletados a partir do site Viva Real, estruturados no formato de um Dataframe Pandas e então
adicionados ao arquivo Dataset.csv aqui no github. a estrutura da tabela gerada é a seguinte:

``` python
{
 'data':'datetime64[ns]',
 'fonte':'str',
 'descricao':'str',
 'endereco':'str',
 'rua':'str',
 'numero':'int',
 'bairro':'str',
 'cidade':'str',
 'valor':'float',
 'periodicidade':'str',
 'condominio':'float',
 'area':'float',
 'qtd_banheiros':'int',
 'qtd_quartos':'int',
 'qtd_vagas':'int',
 'url':'str'
}
```
Uma vez gravados, os dados são disponibilizados no Kaggle em um Dataset Público:

[![Notebook](https://img.shields.io/badge/Acesse_o_dataset-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/datasets/caueausec/florianpolis-rent-pricing-dataset)

Além do Dataset Público, há um notebook Kaggle com exemplos de análises de dados:

[![Notebook](https://img.shields.io/badge/Acesse_o_Notebook-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/code/caueausec/florianopolis-rent-pricing-dataset-eda)

### Estrutura do Projeto:
```
- code/ # Pasta com os arquivos de código do projeto
    - florianópolis_rent_prince_web_scraping.ipynb --> Notebook de extração de dados
    - Florianopolis_rent_pricing_dataset_eda.ipynb --> Notebook com exemplo de análise

- data/ # Pasta com os arquivos de dados (output
    - dataset.csv # Dataset completo do projeto

- docs/ # Pasta de documentações

- maps/ # Pasta para arquivos de mapa do projeto.
``````
### Tags de tecnologias utilizadas:

![Python](https://img.shields.io/badge/python-00BF6F?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%237856FF.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-%237856FF.svg?style=for-the-badge&logo=plotly&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-%237856FF.svg?style=for-the-badge&logo=Matplotlib&logoColor=white)
![Web_Scraping](https://img.shields.io/badge/Web_Scraping-%237856FF.svg?style=for-the-badge&logo=Matplotlib&logoColor=white)
![Kaggle](https://img.shields.io/badge/Kaggle-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)