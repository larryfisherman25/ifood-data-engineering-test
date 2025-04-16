import logging
import boto3
from modules.log_decorator import log_decorator

class S3DataLoader:
    """
    Responsável por carregar arquivos para um bucket S3.
    Essa classe pode ser utilizada tanto para upload dos arquivos raw
    quanto para gravar os dados transformados.
    """
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    @log_decorator
    def upload_file(self, local_file_path: str, s3_file_key: str):
        """
        Faz o upload de um arquivo local para o bucket S3.
        :param local_file_path: Caminho do arquivo local.
        :param s3_file_key: Nome do arquivo no S3.
        """
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_file_key)
            logging.info("Upload concluído: %s para o bucket %s", s3_file_key, self.bucket_name)
        except Exception as e:
            logging.error("Erro ao fazer upload de %s para o bucket %s: %s", s3_file_key, self.bucket_name, e)

    @log_decorator
    def load_transformed_data(self, spark_df, s3_output_prefix: str):
        """
        Grava o DataFrame do Spark no bucket S3 de produção, particionando os dados.
        :param spark_df: DataFrame do Spark transformado.
        :param s3_output_prefix: Prefixo (pasta) de destino no bucket S3.
        """
        output_path = f"s3a://{self.bucket_name}/{s3_output_prefix}"
        spark_df.write.mode("overwrite").parquet(output_path)
        logging.info("Dados transformados escritos em: %s", output_path)
