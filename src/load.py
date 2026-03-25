from sqlalchemy import create_engine, text
import pandas as pd
import os

# Save to csv
def save_dimensions_to_csv(target_file, **dataframes):

    for name, df in dataframes.items():
        file_path = os.path.join(target_file, f"{name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved: {file_path}")