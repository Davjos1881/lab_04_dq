import pandas as pd
import numpy as np
import io
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
import re
from pathlib import Path
from log import log_progress
from extract import extract_raw_data, data_profiling
from validate_input import input_data_validation
from cleaning import clean_data
from transform import transform_data
from validate_output import output_data_validation
from dimensional_model import transform_data_DM
from load import load_to_dw

base_path = Path(r"C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\lab_04\lab_04_dq-main\lab_04_dq-main")

raw_file = base_path / "data" / "raw" / "retail_etl_dataset.csv"
log_file = base_path / "logs" / "log_file.txt"
inp_validate = base_path / "reports"

clean_file = base_path / "data" / "processed" / "retail_clean.csv"
transformed_file = base_path / "data" / "processed" / "retail_transformed.csv"
dims_test = base_path / "data" / "dims_test" 

def main():

    log_progress("Starting ETL process", log_file)


    log_progress("Extract phase started", log_file)
    df_raw = extract_raw_data(raw_file)

    log_progress("Data profiling", log_file)
    data_profiling(df_raw)

    log_progress("Input validation data", log_file)
    input_data_validation(df_raw, inp_validate)

    log_progress("Cleaning data", log_file)       
    df_clean = clean_data(df_raw)                    
    df_clean.to_csv(clean_file, index=False)         
    log_progress("Clean data saved", log_file)

    log_progress("Transforming data", log_file)
    df_transformed = transform_data(df_clean)
    df_transformed.to_csv(transformed_file, index=False)
    log_progress("Transformed data saved", log_file)

    log_progress("Output validation", log_file)
    output_data_validation(df_transformed, inp_validate)
    log_progress("Output validation done", log_file)       

    log_progress("Create dimensional model", log_file)
    df_dimensional_model = transform_data_DM(df_transformed)

    # Save to csv to check first
    for nombre, tabla in df_dimensional_model.items():
        tabla.to_csv(dims_test / f"{nombre}.csv", index=False)
    log_progress("Saved dimensional model", log_file)

    log_progress("Load process started", log_file)
    load_to_dw(df_dimensional_model)
    log_progress('Load phase complete', log_file)


    log_progress('ETL process finished successfully', log_file)

if __name__ == "__main__":
    main()