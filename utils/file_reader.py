import polars as pl 
import os
from utils.path_util import getFullInputPath
from utils.logger import logger

def createDataframe(filename: str) -> pl.DataFrame:
    """
    1. Construct full path from INPUT_DIR + filename.
    2. Read into a Polars DataFrame (CSV, JSON, Excel, Parquet, TSV, IPC).
    3. Prefix all column names with '<filename>__' to ensure uniqueness.
    """
    full_file_path = getFullInputPath(filename)
    ext = os.path.splitext(full_file_path)[-1].lower() 

    if not os.path.exists(full_file_path):
        raise FileNotFoundError(f"File not found: {full_file_path}")

    if ext == ".csv":
        df = pl.read_csv(full_file_path)
    elif ext == ".json":
        df = pl.read_json(full_file_path)
    elif ext in [".xls", ".xlsx"]:
        df = pl.read_excel(full_file_path)
    elif ext == ".parquet":
        df = pl.read_parquet(full_file_path)
    elif ext == ".tsv":
        df = pl.read_csv(full_file_path, separator="\t")
    elif ext in [".ipc", ".feather"]:
        df = pl.read_ipc(full_file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    # Prefix columns: e.g., "age" → "file.csv__age"
    prefixed = df.rename({col: f"{filename.split('.')[0]}__{col}" for col in df.columns})
    return prefixed