import os
import shutil
from dotenv import load_dotenv
from minio import Minio
from kaggle.api.kaggle_api_extended import KaggleApi

load_dotenv("../config/.env")

# Carga credenciales MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')

# Conecta con MinIO
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # cambia a True si usas https
)

# Configura y autentica Kaggle API
api = KaggleApi()
api.authenticate()

# Descarga dataset desde Kaggle
dataset = "asinow/diabetes-dataset"  # cambiar si es necesario
download_path = "/tmp/diabetes_dataset"

os.makedirs(download_path, exist_ok=True)
api.dataset_download_files(dataset, path=download_path, unzip=True)

# Sube archivos descargados a MinIO (RAW)
for file_name in os.listdir(download_path):
    file_path = os.path.join(download_path, file_name)
    minio_path = f"raw/diabetes_dataset/{file_name}"

    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=minio_path,
        file_path=file_path
    )
    print(f"Archivo subido a MinIO: {minio_path}")
    
# Limpieza de archivos temporales
shutil.rmtree(download_path)
print(f"Carpeta temporal '{download_path}' borrada correctamente.")

print("Proceso terminado correctamente.")