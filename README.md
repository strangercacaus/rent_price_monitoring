# Florianópolis Rent Price Monitoring
![Status](https://img.shields.io/badge/Status-em_desenvolvimento-0090ff?style=for-the-badge&logoColor=white)
[![CodeFactor](https://www.codefactor.io/repository/github/strangercacaus/florianopolis_rent_pricing_monitoring/badge/main?style=for-the-badge)](https://www.codefactor.io/repository/github/strangercacaus/florianopolis_rent_pricing_monitoring/overview/main)
![Status](https://img.shields.io/badge/colaboração-livre-purple?style=for-the-badge&logoColor=white)

### Objetivo:

O objetivo deste projeto é capturar recorrentemente dados dos preços de aluguéis ativos em Florianópolis-SC e disponibilizar para analists de dados e interessados da região.

### Inspiração:

A ideia inicial do projeto veio de um exercício de ingestão de dados do bootcamp de engenharia de dados da How Education em 2023 que consistia em realizar o crawling do site vivareal e recuperar informações referentes aos imóveis.

Ao longo do tempo percebi que não havia uma base de dados pública que tivesse dados históricos dos valores de imóveis anunciados nos portais e por este motivo o objetivo deste projeto é expandir o estudo inicial realizando esta captura de dados de modo recorrente e abertamente disponível.

### Escopo:

O projeto se baseia na realização de uma rotina semanal de web-scraping utilizando a plataforma Kaggle.

Uma vez gravados, os dados são disponibilizados no Kaggle em um Dataset Público.

Além do Dataset Público, há um notebook Kaggle com exemplos de análises de dados.

[![Notebook](https://img.shields.io/badge/SCRIPT_WEB_SCRAPING-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/code/caueausec/florian-polis-rent-pricing-dataset-web-scraping)
[![Notebook](https://img.shields.io/badge/dataset-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/datasets/caueausec/florianpolis-rent-pricing-dataset)
[![Notebook](https://img.shields.io/badge/ANÁLISE_EXPLORATÓRIA-035a7d?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/code/caueausec/florianopolis-rent-pricing-dataset-eda)

Os dados são coletados a partir do site Viva Real, estruturados no formato de um Dataframe Pandas e então
adicionados ao arquivo Dataset.csv aqui no github.

### Estrutura de classes

``` mermaid
classDiagram
    Extractor <-- ResultSet
    Ingestor <-- s3
    Ingestor <-- selenium

    class ResultSet{
        
        data : datetime64[ns],
        fonte : str,
        descricao : str,
        endereco : str,
        rua : str,
        numero : int,
        bairro : str,
        cidade : str,
        valor : float,
        periodicidade : str,
        condominio : float,
        area : float,
        qtd_banheiros : int,
        qtd_quartos : int,
        qtd_vagas : int,
        url : str
        to_csv()
        to_parquet()
    }
    class Ingestor{
        city : str
        endpoint: str
        ingest_pages()  :  None
    }
    class Extractor{
        city  :  str
        endpoint: str
        result_set  :  ResultSet
        extract_value() : str
        load_extractor() : lambda
        format_listing() : dict
        parse_html()  :   bs4.Beautifulsoup
        extract_listings_from_soup()  :  bs4.ResultSet
        append_formatted_listing()  :  bool
        process_file()  :  None
        process_folder()
    }
    class GithubApi{
        str token : str
        str owner : str
        str repo : str
        str base_url : str
        str get_url() : str
        str get_file_info() : str
        download_current_content() : pd.DataFrame
        _append_new_content() : pd.DataFrame
        _get_encoded_content() : bytes
        _put_content() : None
        _update_file_content() : None
    }

```
### Estrutura do Projeto:
```
root
├── code/ Pasta com os arquivos de código do projeto.
│   ├── apis.py
│   ├── tests.ipynb
│   ├── florianopolis-rent-pricing-dataset-web-scrapping.ipynb
│   └── florianopolis-rent-pricing-dataset-eda.ipynb
│
├── data/ Pasta com os arquivos de dados do projeto.
│   └── dataset.csv
│
├── docs/
│
├── maps/ Pasta com os arquivos de mapas do projeto.
│   ├── gvw_bairros/
│   ├── gvw_distritos_administrativos/
│   └── regioes_administrativas/
│   
├── README.MD
├── changelog.md # Resumo das últimas mudanças no código.
├── LICENSE.txt
└── requirements.txt
``````
### Tecnologias utilizadas:

![Python](https://img.shields.io/badge/python-%237856FF?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%237856FF.svg?style=for-the-badge&logo=pandas&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-%237856FF.svg?style=for-the-badge&logo=plotly&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-%237856FF.svg?style=for-the-badge&logo=Matplotlib&logoColor=white)
![Web_Scraping](https://img.shields.io/badge/Web_Scraping-%237856FF.svg?style=for-the-badge&logo=Matplotlib&logoColor=white)
![Kaggle](https://img.shields.io/badge/Kaggle-7856FF?style=for-the-badge&logo=kaggle&logoColor=white)
![Geopandas](https://img.shields.io/badge/beautifulsoup-7856FF?style=for-the-badge&logoColor=white)