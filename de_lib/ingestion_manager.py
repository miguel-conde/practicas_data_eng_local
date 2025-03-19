import os
from kaggle.api.kaggle_api_extended import KaggleApi
from .tracer import tracer
import requests


class IngestionManagerBase():
    def download_dataset(self, dataset, download_path):
        raise NotImplementedError


class KaggleIngestionManager(IngestionManagerBase):
    def __init__(self):
        self.api = KaggleApi()
        self.api.authenticate()

    def download_dataset(self, dataset, download_path):
        try:
            tracer.info(f"Downloading dataset {dataset} to {download_path}")
            os.makedirs(download_path, exist_ok=True)
            self.api.dataset_download_files(dataset, path=download_path, unzip=True)
        except Exception as e:
            tracer.error(e)
            raise
        
class URIIngestionManager(IngestionManagerBase):
    def __init__(self):
        pass

    def download_dataset(self, dataset, download_path):
        try:
            tracer.info(f"Downloading dataset {dataset} to {download_path}")
            os.makedirs(download_path, exist_ok=True)
            response = requests.get(dataset)
            response.raise_for_status()
            with open(os.path.join(download_path, os.path.basename(dataset)), 'wb') as f:
                f.write(response.content)
        except Exception as e:
            tracer.error(e)
            raise
        
class IngestionManager():
    def get_ingestion_manager(self, ingestion_type: str):
        try:
            tracer.info(f"Creating ingestion manager for type: {ingestion_type}")
            if ingestion_type == "kaggle":
                return KaggleIngestionManager()
            elif ingestion_type == "uri":
                return URIIngestionManager()
            else:
                raise ValueError(f"Invalid ingestion type: {ingestion_type}")
        except Exception as e:
            tracer.log_exception(e)
            raise