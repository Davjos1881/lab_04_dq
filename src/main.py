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

base_path = Path("C:/Users/santa/Desktop/ETL_cositas/lab_04")

raw_file = base_path / "data" / "raw" / "retail_etl_dataset.csv"
log_file = base_path / "logs" / "log_file.txt"
inp_validate = base_path / "reports" 

def main():

    log_progress("Starting ETL process", log_file)

    log_progress("Extract phase started", log_file)

    df_raw = extract_raw_data(raw_file)

    log_progress("Data profiling", log_file)
    data_profiling(df_raw)

    log_progress("Input validation data", log_file)
    input_data_validation(df_raw, inp_validate)
    



if __name__ == "__main__":
    main()

