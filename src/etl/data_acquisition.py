import os
import logging
import requests

class ParquetDownloader:
    def __init__(self):
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logging.basicConfig(
            filename='data_acquisition_log.txt',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self.__class__.__name__)

    def download_file(self, url: str, dest_path: str):
        """
        Faz o download do arquivo Parquet a partir de uma URL direta e salva no caminho de destino.
        Se o arquivo já existir, ignora o download.
        """
        if not os.path.exists(os.path.dirname(dest_path)):
            os.makedirs(os.path.dirname(dest_path))
            self.logger.info("Diretório criado: %s", os.path.dirname(dest_path))
        
        if os.path.exists(dest_path):
            self.logger.info("Arquivo já existe, ignorando download: %s", dest_path)
            return
        
        try:
            self.logger.info("Iniciando download de: %s", url)
            # Usando stream para lidar com arquivos grandes
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Levanta exceções para códigos de status HTTP de erro

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            self.logger.info("Download concluído: %s", dest_path)
        except Exception as e:
            self.logger.error("Erro ao baixar o arquivo de %s: %s", url, e)

# Exemplo de uso:
if __name__ == "__main__":
    downloader = ParquetDownloader()
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    dest_file = "data/raw_files/yellow_tripdata_2023-01.parquet"
    downloader.download_file(url, dest_file)
