import os
import requests
import logging
from urllib.parse import urljoin
from modules.log_decorator import log_decorator

class RawDataCollector:
    """
    Responsável por baixar arquivos Parquet a partir de URLs,
    salvar localmente, fazer o upload para o bucket raw no S3
    e, após o upload, remover os arquivos locais.
    Se o arquivo 'download_history.txt' existir, os downloads e uploads são pulados.
    """
    def __init__(self, base_url="", download_dir="data"):
        self.base_url = base_url
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        self.history_file = os.path.join(self.download_dir, "download_history.txt")
    
    @log_decorator
    def has_already_processed(self) -> bool:
        """
        Retorna True se o arquivo de histórico já existir.
        """
        return os.path.exists(self.history_file)
    
    @log_decorator
    def download_file(self, file_url: str) -> str:
        """
        Faz o download de um arquivo Parquet a partir de file_url.
        Retorna o caminho local do arquivo baixado.
        """
        # Se file_url não for completo, constrói a URL com base em base_url.
        full_url = file_url if file_url.startswith("http") else urljoin(self.base_url, file_url)
        local_file = os.path.join(self.download_dir, os.path.basename(full_url))
        if os.path.exists(local_file):
            logging.info("Arquivo já existe localmente: %s", local_file)
            return local_file
        try:
            logging.info("Realizando download do arquivo: %s", full_url)
            response = requests.get(full_url, stream=True)
            response.raise_for_status()
            with open(local_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info("Download concluído: %s", local_file)
            return local_file
        except Exception as e:
            logging.error("Erro ao baixar %s: %s", full_url, e)
            return None

    @log_decorator
    def process_files(self, file_urls: list, s3_loader):
        """
        Para cada URL na lista, verifica se o arquivo de histórico existe.
        Se não existir, baixa o arquivo, faz o upload via s3_loader
        e deleta o arquivo local. Ao final, cria o arquivo de histórico.
        :param file_urls: Lista de URLs dos arquivos Parquet.
        :param s3_loader: Instância de S3DataLoader para upload dos arquivos.
        """
        if self.has_already_processed():
            logging.info("Arquivo de histórico encontrado. Pulando downloads e uploads.")
            return

        for file_url in file_urls:
            local_file = self.download_file(file_url)
            if local_file:
                s3_loader.upload_file(local_file, os.path.basename(local_file))
                # try:
                #     os.remove(local_file)
                #     logging.info("Arquivo local removido: %s", local_file)
                # except Exception as e:
                #     logging.error("Erro ao remover arquivo %s: %s", local_file, e)
        
        # Após processar todos os downloads, cria o arquivo de histórico.
        try:
            with open(self.history_file, "w") as hist_file:
                hist_file.write("Arquivos baixados e enviados em processo concluído.")
            logging.info("Arquivo de histórico criado: %s", self.history_file)
        except Exception as e:
            logging.error("Erro ao criar arquivo de histórico %s: %s", self.history_file, e)
