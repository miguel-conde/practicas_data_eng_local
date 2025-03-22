from .dl_manager import DLManager, MinioDLManager
from .tracer import tracer

class CleaningManager:
    
    def __init__(self, dl_manager: DLManager = MinioDLManager()):
        self.dl_manager = dl_manager
    
    def clean_process(self, cleaning_list):
        for cleaning_info in cleaning_list:
            raw_object_name   = cleaning_info["raw_object_name"]
            clean_object_name = cleaning_info["clean_object_name"]
            cleaning_function = cleaning_info["cleaning_function"]
        
            # Lee el CSV desde Data Lake (raw)
            raw_object_name = raw_object_name
            df = self.dl_manager.get_csv(raw_object_name)
            
            # Data cleaning
            df_clean = cleaning_function(df)
            
            # Guarda el resultado limpio en Data Lake (clean)
            self.dl_manager.put_csv(df_clean, clean_object_name)
            
            tracer.info(f"Clean dataset successfully saved in MinIO: {clean_object_name}")