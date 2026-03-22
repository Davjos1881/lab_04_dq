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

base_path = Path(r"C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\lab_04\lab_04_dq-main\lab_04_dq-main")

raw_file      = base_path / "data" / "raw" / "retail_etl_dataset.csv"
log_file      = base_path / "logs" / "log_file.txt"

def main():

    log_progress("Starting ETL process", log_file)

    log_progress("Extract and Profiling phase started", log_file)

    df_raw = extract_raw_data(raw_file)
    df_profiling = data_profiling(df_raw)
    



if __name__ == "__main__":
    main()

