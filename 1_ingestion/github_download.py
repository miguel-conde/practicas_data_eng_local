import os
import shutil
from dotenv import load_dotenv, find_dotenv
from de_lib.tracer import tracer

from de_lib.ingestion_manager import URIIngestionManager
from de_lib.dl_manager import MinioDLManager

load_dotenv(find_dotenv())


def download_github_dataset(file, download_path, minio_path):
    tracer.info(f"Descargando dataset {file} a {download_path}")
    
    os.makedirs(download_path, exist_ok=True)
    uri_ingestion_manager.download_dataset(file, download_path)

    # Sube archivos descargados a MinIO (RAW)
    for file_name in os.listdir(download_path):
        file_path = os.path.join(download_path, file_name)
        minio_path = os.path.join(minio_path, file_name)

        dl_manager.put_file(file_path, minio_path)
        tracer.info(f"Archivo subido a MinIO: {minio_path}")
        
    # Limpieza de archivos temporales
    shutil.rmtree(download_path)
    tracer.info(f"Carpeta temporal '{download_path}' borrada correctamente.")

    tracer.info("Proceso terminado correctamente.")

if __name__ == "__main__":
    uri_ingestion_manager = URIIngestionManager()
    dl_manager = MinioDLManager()
    
    download_list = [
        {
            "file": "https://raw.githubusercontent.com/miguel-conde/practicas_data_eng_local/refs/heads/main/data/raw/diabetes_dataset.csv",
            "download_path": "/tmp/diabetes_dataset",
            "minio_path": "raw/diabetes_dataset/",
        }
    ]
    
    for dataset in download_list:
        download_github_dataset(dataset['file'], dataset['download_path'], dataset['minio_path']) 
        
    tracer.info("Proceso terminado correctamente.")