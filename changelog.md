## 09/11/2023:

### Web_Scraping:
- O armazenamento de dados do pipeline foi transferido para o Amazon S3
- O esquema de arquivos foi refatorado focando em manter o volume alto de dados no S3 e uma quantidade útil de exemplos de dados crus de todas as etapas no Github.
- O script main agora realiza cada etapa da rotina de extração individualmente:
  - Extração: Obtém todas as páginas do portal em HTML com a utilização do Selenium e salva os arquivos resultantes na camada RAW do pipeline no S3.
  - Processamento: Vasculha cada arquivo html para obter as tags HTML configuradas no método format_listings da classe de API e armazena no formato apache parquet na camada Processed no S3.
  - Carregamento: Acrescenta o arquivo da ultima data no arquivo parquet consolidado na camada curated no S3.

## 28/10/2023:

### Web_Scraping:
- Implementada abstração de classes na api VivaRealApi
- Primeira versão de testes black box implementada
- Consumo de apis modularizado implementado no Kaggle
- Criação do mapa de relacionaemnto de classes


## 27/10/2023:

### Web_Scraping:

- Corrigido bug que fazia com que o objeto de api ingerisse os mesmos anúncios recorrentemente para o result_set.
- Inclusão de uma variação aleatória no número de página gerado para as novas requisições com base no result_count e número de registros da primeira requisição.
- Variável de ciclos de ingestão do método ._ingest_listings() renomeado de pages_number para max_attempts
- Incluído contador de tentativas de ingestão no método ._ingest_listings()
- Incluído contador de quantidade de novos anúncios ingeridos no método ._ingest_current_page

### EDA:

- Alguns plots tiveram a engine substituída do plotly para o matplotlib
- Os distplots de variaveis quantitativas foram transformados em um subplot grid do matplotlib
