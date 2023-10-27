## 27/10/2023:

### Web_Scrapping:

- Corrigido bug que fazia com que o objeto de api ingerisse os mesmos anúncios recorrentemente para o result_set.
- Inclusão de uma variação aleatória no número de página gerado para as novas requisições com base no result_count e número de registros da primeira requisição.
- Variável de ciclos de ingestão do método ._ingest_listings() renomeado de pages_number para max_attempts
- Incluído contador de tentativas de ingestão no método ._ingest_listings()
- Incluído contador de quantidade de novos anúncios ingeridos no método ._ingest_current_page

### EDA:

- Alguns plots tiveram a engine substituída do plotly para o matplotlib
- Os distplots de variaveis quantitativas foram transformados em um subplot grid do matplotlib
