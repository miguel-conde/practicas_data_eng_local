import os
from sqlalchemy import create_engine, text

from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = str(os.getenv("POSTGRES_PASSWORD"))
DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DBNAME")

# engine = create_engine("postgresql://usuario:password@servidor/postgres_db")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

with engine.connect() as conn:
    schema_file_path = os.path.join("C:\\", "Users", "migue", "Documents", "PROYECTOS DATA SCIENCE", "practicas_data_eng_local", "sql", "schema_diabetes_dataset.sql")
    with open(schema_file_path) as file:
        query = file.read()
    # os.system(f'psql -U {DB_USER} -h {DB_HOST} -d {DB_NAME} -c "\\copy ({query}) TO stdout"')

    # # GRANT pg_read_server_files TO your_username;
        
    with conn.begin() as transaction:
        try:
            conn.execute(query)
        except Exception as e:
            transaction.rollback()
            print(f"An error occurred: {e}")
            # raise Exception("An error occurred while creating the tables")
        else:
            transaction.commit()
            print("Tables created successfully")
                    
# engine.close()