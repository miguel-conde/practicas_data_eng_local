from dl_manager import DLManager

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

def clean_process(cleaning_list):
    
    dl_manager = DLManager()
    
    for cleaning_info in cleaning_list:
        raw_object_name   = cleaning_info["raw_object_name"]
        clean_object_name = cleaning_info["clean_object_name"]
        cleaning_function = cleaning_info["cleaning_function"]
    
        # Lee el CSV desde MinIO (raw)
        raw_object_name = raw_object_name
        df = dl_manager.get_csv(raw_object_name)
        
        # Data cleaning
        df_clean = cleaning_function(df)
        
        # Guarda el resultado limpio en MinIO (clean)
        dl_manager.put_csv(df_clean, clean_object_name)
        
        print(f"Dataset limpio guardado correctamente en MinIO: {clean_object_name}")
    
      
if __name__ == "__main__":
    
    cleaning_list = [
        {
            "raw_object_name": "raw/diabetes_dataset/diabetes_dataset.csv",
            "clean_object_name": "clean/diabetes_dataset/diabetes_dataset_clean.csv",
            "cleaning_function": clean_kaggle_diabetes_dataset
        }
    ]
    
    clean_process(cleaning_list = cleaning_list)
    