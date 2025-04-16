import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, udf
from pyspark.sql.types import LongType
from modules.log_decorator import log_decorator

# Define uma função simples para converter VendorID para long,
# com tratamento de exceção, se necessário.
def to_long(v):
    try:
        return int(v)
    except Exception:
        return None

to_long_udf = udf(to_long, LongType())

class DataTransformer:
    """
    Responsável por transformar e limpar os dados do DataFrame do Spark.
    """
    @log_decorator
    def transform(self, df: DataFrame) -> DataFrame:
        logging.info("Iniciando transformação dos dados.")
        transformed_df = df.select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss").alias("pickup_datetime"),
            to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").alias("dropoff_datetime")
        ).withColumn("year", year(col("pickup_datetime"))) \
         .withColumn("month", month(col("pickup_datetime")))
        logging.info("Transformação concluída.")
        return transformed_df
