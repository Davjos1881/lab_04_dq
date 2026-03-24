import pandas as pd
import numpy as np
from ydata_profiling import ProfileReport

# Extract raw data
def extract_raw_data(path):
    df_raw = pd.read_csv(path)
    return df_raw

def data_profiling(df_raw):

    # Check first rows
    print("\nCheck data first 5 rows:")
    print(df_raw.head(5))

    # Chek df dimensions
    print("\nShape or df dimensions:", df_raw.shape)
    print("Columns:")
    print(df_raw.columns.tolist())

    # Check data types
    print("\nData types:")
    print(df_raw.dtypes)

    # Check memory use
    memory = df_raw.memory_usage(deep=True).sum() / (1024**2)
    print("\nMemory usage (MB):", round(memory, 2))

    # Check missing value counts and percentage 
    print("\nMissing values:")
    print(df_raw.isnull().sum())
    print("\nMissing values percentage:")
    print((df_raw.isnull().sum() / len(df_raw) * 100).round(2))

    # Check cardinality of categorical columns or unique values
    print("\nCardinality of categorical columns:")
    categorical_cols = ["product", "country", "invoice_date"]

    for col in categorical_cols:
        if col in df_raw.columns:
            print(f"\n{col}:")
            print("Unique values:", df_raw[col].nunique())
 

    # Check descriptive statistics
    print("\nDescribe data:")
    print(df_raw.describe())

    # Check duplicated values in invoiced_id column
    print("\nDuplicated values in column invoiced_id")
    print(df_raw['invoice_id'].value_counts())
    
    # Check total revenue with tolerance ±0.01
    calc = df_raw["quantity"] * df_raw["price"]
    invalid = np.abs(df_raw["total_revenue"] - calc) > 0.01

    print("Invalid total_revenue:")
    print(invalid.sum())

    # Check distribution of invoice_date formats
    print("\nDistribution of invoice_date formats:")
    
    def detect_format(x):
        if pd.isnull(x):
            return "null"
        x = str(x)

        if "-" in x and len(x.split("-")[0]) == 4:
            return "YYYY-MM-DD"
        elif "/" in x and len(x.split("/")[0]) == 4:
            return "YYYY/MM/DD"
        elif "-" in x and len(x.split("-")[0]) == 2:
            return "DD-MM-YYYY"
        else:
            return "other"

    formats = df_raw["invoice_date"].apply(detect_format)
    print(formats.value_counts())

    # Check future dates 
    parsed = pd.to_datetime(df_raw["invoice_date"], errors="coerce")
    print("\nFuture dates:")

    parsed_dates = pd.to_datetime(df_raw["invoice_date"], errors="coerce")
    future_dates = parsed_dates > pd.Timestamp("2023-12-31")
    
    print(future_dates.sum())

    # Check null-like dates
    print("\nNull-like dates:")
    print(parsed.isnull().sum())

    # Generate a profile report 
    profile = ProfileReport(df_raw, title="Lab 04 data profiling report")

    # Or save the report to an HTML file
    profile.to_file(r"C:\Users\santa\Desktop\ETL_cositas\lab_04\reports\data_prof_report_lab04.html")


    return df_raw
