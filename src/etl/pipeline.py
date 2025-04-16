import sys
import os
import glob
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from modules.raw_data_collector import RawDataCollector
from modules.data_transformer import DataTransformer
from modules.s3_data_loader import S3DataLoader
import logging

from pyspark.sql.types import StructType, StructField, LongType, TimestampType, DoubleType

# Adiciona o diretório raiz do projeto (dois níveis acima do pipeline.py) ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def init_spark(app_name="NYC_Taxi_ETL_Pipeline"):
    """
    Inicializa a sessão Spark com as configurações necessárias para acessar o S3.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.parquet.enableVectorizedReader","false") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def read_and_unify_parquet(spark, directory):
    """
    Lê individualmente todos os arquivos Parquet na pasta especificada,
    força a conversão da coluna VendorID para integer de forma especial para
    o arquivo de janeiro (yellow_tripdata_2023-01.parquet), e então unifica
    todos os DataFrames.
    Retorna um DataFrame unificado contendo apenas as colunas necessárias:
       VendorID, passenger_count, total_amount,
       tpep_pickup_datetime e tpep_dropoff_datetime.
    """
    # Lista somente arquivos .parquet
    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))
    df_list = []
    for file in parquet_files:
        # Lê cada arquivo
        df_temp = spark.read.parquet(file)
        # Se for o arquivo de janeiro (que vem com VendorID como long), forçamos o cast para int
        if "2023-01.parquet" in file.lower():
            df_temp = df_temp.withColumn("VendorID", F.col("VendorID").cast("int"))
        else:
            # Para os demais, garante o cast também (caso por acaso haja inconsistência)
            df_temp = df_temp.withColumn("VendorID", F.col("VendorID").cast("int"))
        # Seleciona somente as cinco colunas necessárias para a análise
        df_temp = df_temp.select(
            "VendorID",
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        )
        df_list.append(df_temp)
    if df_list:
        # Realiza o union de todos os DataFrames
        unified_df = reduce(lambda a, b: a.unionByName(b), df_list)
        return unified_df
    else:
        return None

def main():
    logging.basicConfig(level=logging.INFO)
    spark = init_spark()
    
    # Buckets configurados (criados via Terraform)
    raw_bucket = "yellow-taxi-files-larry-test"  # Bucket para arquivos raw
    prod_bucket = "prd-yellow-taxi-table-larry-test"  # Bucket para dados transformados
    
    # Instância do S3DataLoader para upload dos arquivos raw
    raw_loader = S3DataLoader(raw_bucket)
    
    # Lista de URLs dos arquivos Parquet para os meses de janeiro a maio de 2023
    file_urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet"
    ]
    
    # Coleta dos dados: Baixa os arquivos e faz o upload para o bucket raw
    collector = RawDataCollector(download_dir="data")
    collector.process_files(file_urls, raw_loader)    

    # # Após upload dos arquivos raw, leia-os diretamente do bucket raw com o Spark
    # raw_input_path = f"s3a://{raw_bucket}/"
    # logging.info("Lendo dados raw do S3: %s", raw_input_path)

    # Em vez de ler direto do bucket S3, vamos ler os arquivos raw da pasta local
    local_raw_dir = "data"  # ou a pasta específica onde os parquets estão (ex.: "data/raw_files")
    logging.info("Lendo dados raw localmente da pasta: %s", local_raw_dir)
    
    df_raw = read_and_unify_parquet(spark, local_raw_dir)
    df_raw.show(10, truncate=False)
    df_raw.printSchema()

    # Transformação dos dados – sua lógica de transformação continua
    transformer = DataTransformer()
    df_transformed = transformer.transform(df_raw)
    
    # Envio para o bucket de produção
    prod_bucket = "prd-yellow-taxi-table-larry-test"
    prod_loader = S3DataLoader(prod_bucket)
    prod_loader.load_transformed_data(df_transformed, s3_output_prefix="processed")
    
    spark.stop()

if __name__ == "__main__":
    main()