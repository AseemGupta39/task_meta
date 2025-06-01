import os
from pathlib import Path
from dotenv import load_dotenv
# def get_input_path(file_name: str) -> str: 
#     return os.path.join("data/input", file_name)

# def get_output_path(file_name: str) -> str: 
#     return os.path.join("data/output", file_name)

load_dotenv() 

# BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = os.environ.get("INPUT_DIR")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR")

INPUT_DIR = Path(INPUT_DIR)
OUTPUT_DIR = Path(OUTPUT_DIR)
OUTPUT_FILENAME = os.environ.get("OUTPUT_FILENAME")
# print(INPUT_DIR,OUTPUT_DIR)

# Ensure directories exist
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def getFullInputPath(FileName):
    return INPUT_DIR / FileName

def getFullOutputPath():
    return OUTPUT_DIR / OUTPUT_FILENAME

print(getFullInputPath("data1.csv"))