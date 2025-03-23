import os
import psycopg2
import sqlite3
from io import BytesIO
from dotenv import load_dotenv
from de_lib.tracer import tracer


class DWManagerBase():
    def connect(self):
        raise NotImplementedError
    
    def execute_query(self, query, params=None):
        try:
            tracer.info(f"Executing query: {query}")
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception as e:
            tracer.error(e)
            raise

    def fetch_data(self, query, params=None):
        try:
            tracer.info(f"Fetching data from query: {query}")
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            tracer.error(e)
            raise
        
    def copy_from_csv(self, copy_query, df_clean):
        try:
            # Guardar DataFrame temporalmente en memoria
            csv_buffer = BytesIO()
            df_clean.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            self.cursor.copy_expert(copy_query, csv_buffer)
            self.conn.commit()
        except Exception as e:
            tracer.error(e)
            raise

    def close(self):
        try:
            tracer.info(f"Closing DB connection (db: {self.db_name}) host: {self.db_host}, user: {self.db_user})")
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            tracer.error(e)
            raise


class PGManager(DWManagerBase):
    def __init__(self):
        load_dotenv()
        self.db_user = os.getenv('POSTGRES_USER')
        self.db_password = os.getenv('POSTGRES_PASSWORD')
        self.db_name = os.getenv('POSTGRES_DBNAME')
        self.db_host = os.getenv('POSTGRES_HOST')
        self.db_port = os.getenv('POSTGRES_PORT')
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            tracer.info(f"Connecting to PostgreSQL (db: {self.db_name}), host: {self.db_host}, user: {self.db_user}), port: {self.db_port}")
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            tracer.error(e)
            raise


class SQLiteManager(DWManagerBase):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            tracer.info(f"Connecting to SQLite database: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
        except Exception as e:
            tracer.error(e)
            raise
        
# Factory class to create the appropriate DWManager
class DWManager:
    
    @staticmethod
    def create_dw_manager(dw_type: str):
        try:
            tracer.info(f"Creating DW manager for type: {dw_type}")
            if dw_type == "postgres":
                return PGManager()
            elif dw_type == "sqlite":
                return SQLiteManager(db_path=os.getenv('SQLITE_DB_PATH'))
            else:
                raise ValueError(f"Invalid DW type: {dw_type}")
        except Exception as e:
            tracer.error(e)
            raise
        
if __name__ == "__main__":
    
    dw_manager = DWManager.create_dw_manager("postgres")
    dw_manager.connect()
    dw_manager.execute_query("SELECT * FROM information_schema.tables")
    
    print(dw_manager.fetch_data("SELECT * FROM information_schema.tables"))
    
    dw_manager.close()
    
    # dw_manager = DWManager().get_dw_manager("sqlite")
    # dw_manager.connect()
    # dw_manager.execute_query("SELECT * FROM sqlite_master")
    
    # print(dw_manager.fetch_data("SELECT * FROM sqlite_master"))
    
    # dw_manager.close()