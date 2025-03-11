import os
import pandas as pd
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv

load_dotenv("../config/.env")

class DLManager:
    
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