import pandas as pd

def transform_data(df_clean):
    df = df_clean.copy()

    # 1. Standardize country names
    country_map = {
        "colombia": "Colombia", "CO": "Colombia",
        "ecuador": "Ecuador", "EC": "Ecuador",
        "peru": "Peru", "PE": "Peru",
        "chile": "Chile", "CL": "Chile"
    }
    df["country"] = df["country"].replace(country_map)

    # 2. Parse invoice_date and extract time columns
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], dayfirst=True, errors="coerce")
    df["year"]         = df["invoice_date"].dt.year
    df["month"]        = df["invoice_date"].dt.month
    df["day_of_week"]  = df["invoice_date"].dt.day_name()

    # 3. Cast customer_id to nullable integer
    df["customer_id"] = df["customer_id"].astype("Int64")

    # 4. Normalize product names
    df["product"] = df["product"].str.strip().str.title()

    # 5. Feature engineering: revenue bin
    # Divides total_revenue into 3 equal-sized groups (by row count) using quantiles:
    # Low = bottom 33%, Medium = middle 33%, High = top 33%
    df["revenue_bin"] = pd.qcut(
        df["total_revenue"],
        q=3,
        labels=["Low", "Medium", "High"]
    )

    print(f"Rows after transform: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    return df