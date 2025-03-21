import os
import psycopg2
from dotenv import load_dotenv
from io import BytesIO
from de_lib.dl_manager import DLManager
from de_lib.dw_manager import DWManager
from de_lib.tracer import tracer

load_dotenv()

dl_manager = DLManager.create_dl_manager("minio")
dw_manager = DWManager().get_dw_manager("postgres")    

df_clean = dl_manager.get_csv("clean/diabetes_dataset/diabetes_dataset_clean.csv")

dw_manager.connect()

# Crear tabla si no existe
with open('sql/create_table.sql', 'r') as file:
    dw_manager.execute_query(file.read())

# Vaciar tabla destino antes de carga (truncate)
dw_manager.execute_query("TRUNCATE TABLE diabetes_dataset RESTART IDENTITY;")

# Cargar CSV en PostgreSQL usando copy_expert
copy_query = """
COPY diabetes_dataset (Age, Pregnancies, BMI, Glucose, BloodPressure, HbA1c, LDL, HDL, 
    Triglycerides, WaistCircumference, HipCircumference, WHR, FamilyHistory, DietType, 
    Hypertension, MedicationUse, Outcome)
FROM STDIN WITH (FORMAT csv);
"""

# dw_manager.copy_from_csv(copy_query, csv_buffer)
dw_manager.copy_from_csv(copy_query, df_clean)

# Verificación simple de integridad (conteo de filas)
dw_manager.execute_query("SELECT COUNT(*) FROM diabetes_dataset;")
row_count = dw_manager.cursor.fetchone()[0]
tracer.info(f"Filas cargadas en PostgreSQL: {row_count}")

# Check simple (compara conteo filas cargadas vs DataFrame)
expected_rows = len(df_clean)
if row_count == expected_rows:
    tracer.info("✅ Check de integridad pasado correctamente.")
else:
    tracer.warning(f"⚠️ Posible problema: filas esperadas {expected_rows}, filas cargadas {row_count}")

# Limpieza y cierre conexiones
dw_manager.close()
tracer.info("Carga a PostgreSQL terminada correctamente.")



if __name__ == "__main__":
    
    
    load_list = [
        {
            "dl_clean_file": "clean/diabetes_dataset/diabetes_dataset_clean.csv",
            "load_sql_file": "load/diabetes_dataset/load_diabetes_dataset.sql",
        },
    ]