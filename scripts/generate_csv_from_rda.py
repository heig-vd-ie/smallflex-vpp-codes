import rpy2.robjects as ro
import os 
from config import settings
import json

def generate_csv_from_rda(folder_name: str):
    
    for entry in list(os.scandir(folder_name)):
        input_file_name = entry.path
        output_file_name = input_file_name.replace(".rda", ".csv")
        ro.r['load'](input_file_name)
        ro.r['df'].to_csvfile(output_file_name)

if __name__ == "__main__":
    input_file_names: dict[str, str] = json.load(open(settings.INPUT_FILE_NAMES))
    folder_name: str = input_file_names["greis_wsl_data"]
    generate_csv_from_rda(folder_name=folder_name)