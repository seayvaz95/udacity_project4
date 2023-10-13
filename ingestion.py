import pandas as pd
import numpy as np
import os
import json
from datetime import datetime




#############Load config.json and get input and output paths
with open('config.json','r') as f:
    config = json.load(f) 

input_folder_path = config['input_folder_path']
output_folder_path = config['output_folder_path']



#############Function for data ingestion
def merge_multiple_dataframe():
    #check for datasets, compile them together, and write to an output file
    df_list = []
    file_list = []
    for file_name in os.listdir(input_folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_folder_path, file_name)
            df = pd.read_csv(file_path)
            df_list.append(df)
            file_list.append(file_path + '\n')
    os.makedirs(output_folder_path, exist_ok=True)
    final_df = pd.concat(df_list, axis=0, ignore_index=True).drop_duplicates()
    final_df.to_csv(os.path.join(output_folder_path, 'finaldata.csv'), index=False)
    with open(os.path.join(output_folder_path, 'ingestedfiles.txt'), 'w') as f:
        f.writelines(file_list)

if __name__ == '__main__':
    merge_multiple_dataframe()
