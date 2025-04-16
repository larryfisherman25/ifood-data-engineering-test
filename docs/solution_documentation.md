# Solução de Pipeline ETL – Teste Técnico iFood

## 1. Visão Geral

Esta solução implementa um pipeline de dados que integra registros de viagens de táxi amarelo da cidade de Nova York para o período de janeiro a maio de 2023 em um Data Lake House. O pipeline foi desenvolvido em Python com PySpark, seguindo uma abordagem modular e aderindo às melhores práticas de DevOps e engenharia de dados. Além do pipeline, a solução inclui:
- Criação de uma tabela externa no Hive para acesso aos dados processados.
- Um notebook de Análise Exploratória de Dados (EDA) com visualizações e análises estatísticas.

## 2. Arquitetura da Solução

A solução é composta pelos seguintes módulos e componentes:

- **ETL Pipeline**
  - **RawDataCollector**: Responsável por baixar arquivos Parquet a partir de URLs e armazená-los localmente (em uma pasta, por exemplo, `data/`). Utiliza um arquivo de histórico para evitar downloads e uploads redundantes.
  - **DataTransformer**: Seleciona apenas as cinco colunas necessárias para a análise (VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime) e uniformiza o schema (por meio de cast, por exemplo, convertendo VendorID para integer).
  - **S3DataLoader**: Gerencia o upload dos dados processados para o bucket de produção, salvando-os em formato Parquet particionado por ano e mês.
  - **LogDecorator**: Implementa um decorador para registrar logs automaticamente nas funções críticas, evitando repetição de código de logging.

- **Infraestrutura**
  - **Terraform**: Utilizado para provisionar os recursos no AWS S3, criando o bucket para os dados brutos (raw) e o bucket para os dados processados (produção).

- **Tabelas Hive**
  - Foi desenvolvido um script SQL para criação de uma tabela externa no Hive que consome os dados processados do bucket S3, utilizando o formato Parquet e particionamento por ano e mês.

- **Análise de Dados (EDA)**
  - Um Jupyter Notebook foi preparado para realizar uma Análise Exploratória de Dados (EDA) detalhada, explorando estatísticas descritivas, distribuições e agregações específicas:
    - Valor médio (total_amount) arrecadado por mês para o período de 2023 (de janeiro a maio).
    - Média de passageiros (passenger_count) por hora e por dia.
  - Os gráficos foram gerados usando Matplotlib e Seaborn, e a conversão de DataFrame Spark para pandas foi realizada para facilitar a visualização.

## 3. Fluxo do Pipeline

1. **Coleta de Dados (RawDataCollector):**
   - Os arquivos Parquet são baixados a partir das URLs fornecidas, se ainda não houver um arquivo de histórico (download_history.txt) na pasta local.
   - Os arquivos baixados são mantidos localmente na pasta `data/` para serem reutilizados em execuções futuras.
   - Opcionalmente, os arquivos também são enviados para o bucket raw do AWS S3.

2. **Transformação dos Dados (DataTransformer):**
   - São selecionadas somente as colunas de interesse: VendorID, passenger_count, total_amount, tpep_pickup_datetime e tpep_dropoff_datetime.
   - É aplicada a conversão na coluna VendorID para garantir um schema homogêneo (convertendo para integer, conforme os demais arquivos).

3. **Carregamento dos Dados (S3DataLoader):**
   - Os dados transformados são gravados no bucket de produção (`prd-yellow-taxi-table-larry-test`) no prefixo “processed”, em formato Parquet particionado por ano e mês.

4. **Criação da Tabela Hive:**
   - Um script SQL cria uma tabela externa no Hive apontando para o diretório de dados processados no S3.
   - As partições são registradas com o comando `MSCK REPAIR TABLE` para facilitar as consultas.

5. **Análise de Dados (EDA):**
   - Um notebook de Jupyter acessa os dados processados (a partir do bucket ou via leitura local) e realiza análises exploratórias:
     - Agrupamento e cálculo da média de total_amount por mês (filtrado para 2023: janeiro a maio).
     - Agrupamento e cálculo da média de passenger_count por hora (usando a coluna `pickup_datetime`) e por dia.
   - São geradas visualizações como gráficos de barras, linhas e matrizes de correlação para identificar tendências, distribuições e relações entre as variáveis.

## 4. Scripts e Comandos Essenciais

### 4.1 Criação da Tabela Hive

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi (
    VendorID INT,
    passenger_count DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3a://prd-yellow-taxi-table-larry-test/processed/';

MSCK REPAIR TABLE yellow_taxi;
```

### 4.2 Comandos para Execução do Pipeline e Notebook

- Para executar o pipeline ETL:
`poetry run python src/etl/pipeline.py`

- Para iniciar o Jupyter Notebook utilizando o ambiente do Poetry:
`poetry run jupyter notebook`

## 5. Tecnologias e Dependências

    Linguagem e Frameworks:
    • Python
    • PySpark

    Infraestrutura:
    • AWS S3 (para armazenamento raw e processado)
    • Terraform (para provisionamento dos buckets S3)

    Banco de Dados/Metastore:
    • Apache Hive (tabela externa para dados processados)

    Análise e Visualização:
    • Jupyter Notebook
    • Pandas
    • Matplotlib
    • Seaborn

    Gerenciamento de Dependências:
    • Poetry

## 6. Considerações Finais

Esta solução foi construída de forma modular, permitindo fácil manutenção e evolução. O pipeline ETL processa os dados de forma consistente mesmo diante de variações de schema entre os arquivos raw e grava os dados transformados de forma particionada, facilitando consultas futuras. A etapa de Análise Exploratória de Dados (EDA) realizada no Jupyter Notebook demonstra de forma visual os insights dos dados, atendendo aos requisitos do teste técnico e evidenciando a aplicação de boas práticas em engenharia de dados e DevOps.

Esta documentação serve como um guia completo para a execução, entendimento e manutenção da solução. Qualquer dúvida ou necessidade de ajustes pode ser facilmente resolvida com base nos módulos e scripts apresentados no projeto.