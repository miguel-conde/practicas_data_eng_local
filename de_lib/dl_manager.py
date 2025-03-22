import os
import pandas as pd
from minio import Minio
from io import BytesIO
from pathlib import Path
from typing import Literal
import shutil
from .tracer import tracer

# Abstact class
class DLManagerBase:
    
    def get_csv(self, object_name:str) -> pd.DataFrame:
        """
        Retrieve a CSV file from MinIO storage.

        Args:
            object_name (str): The name of the object to retrieve.

        Returns:
            pd.DataFrame: The CSV file content as a pandas DataFrame.
        """
        raise NotImplementedError
    
    def put_csv(self, df: pd.DataFrame, object_name: str):
        """Uploads a DataFrame as a CSV file to a MinIO bucket.

            df (pandas.DataFrame): The DataFrame to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
        raise NotImplementedError
    
    def put_file(self, file_path: str, object_name: str):
        """Uploads a file to the specified MinIO bucket.

            file_path (str): The path to the file to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
        raise NotImplementedError
    
    def file_exists(self, object_name: str) -> bool:
        """
        Check if a file exists in the MinIO bucket.

        Args:
            object_name (str): The name of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        raise NotImplementedError

class MinioDLManager(DLManagerBase):
    
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
        
    def get_csv(self, object_name: str) -> pd.DataFrame:
        """
        Retrieve a CSV file from MinIO storage.

        Args:
            object_name (str): The name of the object to retrieve.

        Returns:
            pd.DataFrame: The CSV file content as a pandas DataFrame.
        """
        try:
            response = self.client.get_object(self._minio_bucket, object_name)
            return pd.read_csv(response)
        except Exception as e:
            tracer.error(f"Error reading CSV file: {object_name}, Error: {e}")
            raise
    
    def put_csv(self, df: pd.DataFrame, object_name: str):
        """Uploads a DataFrame as a CSV file to a MinIO bucket.

            df (pandas.DataFrame): The DataFrame to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
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
        
    def put_file(self, file_path: str, object_name: str):
        """Uploads a file to the specified MinIO bucket.

            file_path (str): The path to the file to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
        with open(file_path, 'rb') as file_data:
            self.client.put_object(
                bucket_name = self._minio_bucket,
                object_name = object_name,
                data        = file_data,
                length      = os.stat(file_path).st_size,
                content_type = 'application/csv'
            )
        
    def file_exists(self, object_name: str) -> bool:
        """
        Check if a file exists in the MinIO bucket.

        Args:
            object_name (str): The name of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        try:
            self.client.stat_object(self._minio_bucket, object_name)
            return True
        except Exception as e:
            return False    
        
class LocalFolderDLManager(DLManagerBase):
    
    def __init__(self, folder_path):
        self.folder_path = folder_path
        Path(self.folder_path).mkdir(parents=True, exist_ok=True)
        
    def get_csv(self, object_name: str) -> pd.DataFrame:
        """
        Retrieve a CSV file from MinIO storage.

        Args:
            object_name (str): The name of the object to retrieve.

        Returns:
            pd.DataFrame: The CSV file content as a pandas DataFrame.
        """
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
    
    def put_csv(self, df: pd.DataFrame, object_name: str):
        """Uploads a DataFrame as a CSV file to a MinIO bucket.

            df (pandas.DataFrame): The DataFrame to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
        file_path = os.path.join(self.folder_path, object_name)
        try:
            tracer.info(f"Writing CSV file: {file_path}")
            df.to_csv(file_path, index=False)
        except Exception as e:
            tracer.error(f"Error writing CSV file: {file_path}, Error: {e}")
            raise
        
    def put_file(self, file_path: str, object_name: str):
        """Uploads a file to the specified MinIO bucket.

            file_path (str): The path to the file to be uploaded.
            object_name (str): The name of the object to be created in the bucket.
        """
        dest_file_path = os.path.join(self.folder_path, object_name)
        try:
            tracer.info(f"Copying file: {file_path} to {dest_file_path}")
            shutil.copyfile(file_path, dest_file_path)
        except Exception as e:
            tracer.error(f"Error copying file: {file_path} to {dest_file_path}, Error: {e}")
            raise
        
    
    def file_exists(self, object_name: str) -> bool:
        """
        Check if a file exists in the MinIO bucket.

        Args:
            object_name (str): The name of the object to check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        file_path = os.path.join(self.folder_path, object_name)
        return os.path.exists(file_path)
        
class DLManager:
    """
    DLManager is a factory class for creating different types of download managers.
    Methods
    -------
    create_dl_manager(manager_type, **kwargs):
        Static method to create and return an instance of a download manager based on the specified type.
        Parameters:
            manager_type (str): The type of download manager to create. Supported types are 'minio' and 'local_folder'.
            **kwargs: Additional keyword arguments to pass to the download manager's constructor.
        Returns:
            An instance of the specified download manager type.
        Raises:
            ValueError: If an unknown manager type is specified.
    """
    
    @staticmethod
    def create_dl_manager(manager_type: Literal['minio', 'local_folder'], **kwargs) -> DLManagerBase:
        """Creates a download manager instance based on the specified type.

            manager_type (Literal['minio', 'local_folder']): The type of download manager to create.
                - 'minio': Creates an instance of MinioDLManager.
                - 'local_folder': Creates an instance of LocalFolderDLManager.
            **kwargs: Additional keyword arguments to pass to the download manager constructor.

            ValueError: If an unknown manager type is provided.

            DLManagerBase: An instance of a download manager based on the specified type.
        """
        if manager_type == 'minio':
            return MinioDLManager(**kwargs)
        elif manager_type == 'local_folder':
            return LocalFolderDLManager(**kwargs)
        else:
            raise ValueError(f"Unknown manager type: {manager_type}")