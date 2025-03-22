from dotenv import load_dotenv
from de_lib.dl_manager import DLManager
from de_lib.dw_manager import DWManager
from de_lib.tracer import tracer

load_dotenv()

def load_dl_to_dw(load_list, dl_manager, dw_manager):
    
    dw_manager.connect()
    
    for load in load_list:
        # Get CSV file from DL
        try:
            df_clean = dl_manager.get_csv(load["dl_data_csv"])
        except Exception as e:
            tracer.error(f"Error loading CSV file: {load['dl_data_csv']}, Error: {e}")
            raise
        # Create table in DW if not exists
        try:
            with open(load["create_table_sql"], 'r') as file:
                dw_manager.execute_query(file.read())
        except Exception as e:
            tracer.error(f"Error creating table in PostgreSQL, Error: {e}")
            raise
        # If needed, empty table in DW
        try:
            with open(load["empty_table_sql"], 'r') as file:
                dw_manager.execute_query(file.read())
        except Exception as e:
            tracer.error(f"Error emptying table in PostgreSQL, Error: {e}")
        # Copy data to DW
        try:
            with open(load["copy_table_sql"], 'r') as file:
                dw_manager.copy_from_csv(file.read(), df_clean)
        except Exception as e:
            tracer.error(f"Error copying data to PostgreSQL, Error: {e}")
            raise
        
        # Simple integrity check (row count)
        tracer.info(f"Verifying integrity of table {load['table_name']} in PostgreSQL...")
        dw_manager.execute_query(f"SELECT COUNT(*) FROM {load['table_name']};")
        row_count = dw_manager.cursor.fetchone()[0]
        tracer.info(f"Rows loaded in PostgreSQL: {row_count}")

        # Simple check (compare loaded row count vs DataFrame)
        expected_rows = len(df_clean)
        if row_count == expected_rows:
            tracer.info("✅ Integrity check passed successfully.")
        else:
            tracer.warning(f"⚠️ Possible issue: expected rows {expected_rows}, loaded rows {row_count}")
            
    dw_manager.close()
    tracer.info("Load to PostgreSQL completed successfully.")


if __name__ == "__main__":
    
    load_list = [
        {
            "table_name": "diabetes_dataset",
            "dl_data_csv": "clean/diabetes_dataset/diabetes_dataset_clean.csv",
            "empty_table_sql": "sql/empty_table_diabetes_dataset.sql",
            "create_table_sql": "sql/create_table_diabetes_dataset.sql",
            "copy_table_sql": "sql/copy_table_diabetes_dataset.sql",
        },
    ]
    
    dl_manager = DLManager.create_dl_manager("minio")
    dw_manager = DWManager.create_dw_manager("postgres")  
    
    load_dl_to_dw(load_list, dl_manager, dw_manager)