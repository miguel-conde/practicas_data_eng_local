import os
import pandas as pd
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
from pathlib import Path
from tracer import tracer

load_dotenv("../config/.env")

# Abstact class
class DLManager:
    
    def get_csv(self, object_name):
        raise NotImplementedError
    
    def put_csv(self, df, object_name):
        raise NotImplementedError
    
    def file_exists(self, object_name):
        raise NotImplementedError

class MinioDLManager(DLManager):
    
    def __init__(self, secure=False):
        # Credenciales MinIO
        MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT')
        MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
        MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
        MINIO_BUCKET     = os.getenv('MINIO_BUCKET')
        
        self._minio_bucket = MINIO_BUCKET
        self._secure       = secure
        
        self.client    = Minio(
            endpoint   = MINIO_ENDPOINT,
            access_key = MINIO_ACCESS_KEY,
            secret_key = MINIO_SECRET_KEY,
            secure     = self._secure
        )
        
    def get_csv(self, object_name):
        response = self.client.get_object(self._minio_bucket, object_name)
        return pd.read_csv(response)
    
    def put_csv(self, df, object_name):
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        self.client.put_object(
            bucket_name  = self._minio_bucket, 
            object_name  = object_name, 
            data         = csv_buffer, 
            length       = len(csv_buffer.getvalue()), 
            content_type = 'application/csv'
            )
        
    def file_exists(self, object_name):
        try:
            self.client.stat_object(self._minio_bucket, object_name)
            return True
        except Exception as e:
            return False    
        
class LocalFolderDLManager(DLManager):
    
    def __init__(self, folder_path):
        self.folder_path = folder_path
        Path(self.folder_path).mkdir(parents=True, exist_ok=True)
        
    def get_csv(self, object_name):
        file_path = os.path.join(self.folder_path, object_name)
        if not os.path.exists(file_path):
            tracer.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"No such file: '{file_path}'")
        try:
            tracer.info(f"Reading CSV file: {file_path}")
            return pd.read_csv(file_path)
        except Exception as e:
            tracer.error(f"Error reading CSV file: {file_path}, Error: {e}")
            raise
    
    def put_csv(self, df, object_name):
        file_path = os.path.join(self.folder_path, object_name)
        try:
            tracer.info(f"Writing CSV file: {file_path}")
            df.to_csv(file_path, index=False)
        except Exception as e:
            tracer.error(f"Error writing CSV file: {file_path}, Error: {e}")
            raise
    
    def file_exists(self, object_name):
        file_path = os.path.join(self.folder_path, object_name)
        return os.path.exists(file_path)
        
class DLManagerFactory:
    
    @staticmethod
    def create_dl_manager(manager_type, **kwargs):
        if manager_type == 'minio':
            return MinioDLManager(**kwargs)
        elif manager_type == 'local_folder':
            return LocalFolderDLManager(**kwargs)
        else:
            raise ValueError(f"Unknown manager type: {manager_type}")