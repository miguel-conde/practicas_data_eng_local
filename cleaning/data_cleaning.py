# import os
# import pandas as pd
# from minio import Minio
# from io import BytesIO
# from dotenv import load_dotenv

from dl_manager import DLManager

# load_dotenv("../config/.env")

# # Credenciales MinIO
# MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
# MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
# MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
# MINIO_BUCKET = os.getenv('MINIO_BUCKET')

def clean_kaggle_diabetes_dataset(df):
    # --------- Limpieza del dataset (tu lógica aquí) ---------

    # `Outcome` is the target variable, must be boolean
    df_clean = df.copy()
    df_clean["Outcome"] = df_clean["Outcome"].astype(bool)

    # `Hypertension` Presence of high blood pressure (1 = Yes, 0 = No). Must also be boolean
    df_clean["Hypertension"] = df_clean["Hypertension"].astype(bool)

    # FamilyHistory [0 1] Indicates if the individual has a family history of diabetes (1 = Yes, 0 = No). Must be a boolean
    df_clean["FamilyHistory"] = df_clean["FamilyHistory"].astype(bool)

    # DietType [0 1 2] Dietary habits (0 = Unbalanced, 1 = Balanced, 2 = Vegan/Vegetarian). Must be a category (0 = "Unbalanced", 1 = "Balanced", 2 = "Vegan/Vegetarian")
    df_clean["DietType"] = df_clean["DietType"].astype("category")
    df_clean["DietType"] = df_clean["DietType"].cat.rename_categories({0: "Unbalanced", 1: "Balanced", 2: "Vegan/Vegetarian"})

    # MedicationUse [1 0] Indicates if the individual is taking medication (1 = Yes, 0 = No). Must be a boolean
    df_clean["MedicationUse"] = df_clean["MedicationUse"].astype(bool)

    # Outcome [0 1] Diabetes diagnosis result (1 = Diabetes, 0 = No Diabetes). Must be a boolean
    df_clean["Outcome"] = df_clean["Outcome"].astype(bool)
    
    return df_clean

# # Conexión a MinIO
# client = Minio(
#     MINIO_ENDPOINT,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     secure=False  # cambia a True si usas https
# )

# # Lee el CSV desde MinIO (raw)
# raw_object_name = "raw/diabetes_dataset/diabetes_dataset.csv"  # Cambia por tu nombre real
# response = client.get_object(MINIO_BUCKET, raw_object_name)
# df = pd.read_csv(response)

# # --------- Limpieza del dataset (tu lógica aquí) ---------

# # `Outcome` is the target variable, must be boolean
# df_clean = clean_kaggle_diabetes_dataset(df)


# # ---------------------------------------------------------

# # Guarda el resultado limpio en MinIO (clean)
# clean_csv = df_clean.to_csv(index=False).encode('utf-8')
# clean_object_name = "clean/diabetes_dataset/diabetes_dataset_clean.csv"

# client.put_object(
#     bucket_name=MINIO_BUCKET,
#     object_name=clean_object_name,
#     data=BytesIO(clean_csv),
#     length=len(clean_csv),
#     content_type='application/csv'
# )

# print(f"Dataset limpio guardado correctamente en MinIO: {clean_object_name}")

def clean_process(raw_object_name, clean_object_name):
    
    dl_manager = DLManager()
    
    # Lee el CSV desde MinIO (raw)
    raw_object_name = raw_object_name
    df = dl_manager.get_csv(raw_object_name)
    
    # Data cleaning
    df_clean = clean_kaggle_diabetes_dataset(df)
    
    # Guarda el resultado limpio en MinIO (clean)
    dl_manager.put_csv(df_clean, clean_object_name)
    
    print(f"Dataset limpio guardado correctamente en MinIO: {clean_object_name}")
    
    
if __name__ == "main":
    
    clean_process(
        raw_object_name   = "raw/diabetes_dataset/diabetes_dataset.csv",
        clean_object_name = "clean/diabetes_dataset/diabetes_dataset_clean.csv"
    )
    
    