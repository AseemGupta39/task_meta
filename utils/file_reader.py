import polars as pl 
import os
from utils.path_util import INPUT_DIR,OUTPUT_DIR,getFullInputPath
# from utils.path_util import get_input_path

def createDataframe(path: str) -> pl.DataFrame: 
    full_file_path = getFullInputPath(path)
    ext = os.path.splitext(full_file_path)[-1].lower() 
    if ext == ".csv": 
        return pl.read_csv(full_file_path) 
    elif ext == ".json": 
        return pl.read_json(full_file_path) 
    elif ext in [".xls", ".xlsx"]: 
        return pl.read_excel(full_file_path) 
    elif ext == ".parquet": 
        return pl.read_parquet(full_file_path) 
    elif ext == ".tsv": 
        return pl.read_csv(full_file_path, separator='\t') 
    elif ext in [".ipc", ".feather"]: 
        return pl.read_ipc(full_file_path) 
    else: 
        raise ValueError(f"Unsupported file type: {ext}")