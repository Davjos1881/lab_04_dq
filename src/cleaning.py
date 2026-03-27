import pandas as pd 

def clean_data(df_raw):
    df = df_raw.copy()
    if "revenue_match" in df.columns:
        df = df.drop(columns=["revenue_match"])

    print(f"Rows BEFORE: {len(df)}")

    before = len(df)
    df = df.drop_duplicates(subset=["invoice_id"], keep="first")
    print(f"Duplicates removed: {before - len(df)}")

    before = len(df)
    df = df[(df["quantity"] >= 1) & (df["price"] > 0)]
    print(f"Negative quantity/price removed: {before - len(df)}")

    before = len(df)
    df["invoice_date"] = df["invoice_date"].replace(["N/A", "NULL", "", "nan"], pd.NA)
    df = df.dropna(subset=["invoice_date"])
    print(f"Null-like dates removed: {before - len(df)}")

    before = len(df)
    temp_dates = pd.to_datetime(df["invoice_date"], errors="coerce")
    df = df[temp_dates <= "2023-12-31"]
    print(f"Future dates removed: {before - len(df)}")

    null_mask = df["customer_id"].isnull()
    max_id = int(df["customer_id"].max())
    df.loc[null_mask, "customer_id"] = range(max_id + 1, max_id + 1 + null_mask.sum())

    bad_revenue = abs(df["total_revenue"] - df["quantity"] * df["price"]) > 0.01
    df.loc[bad_revenue, "total_revenue"] = (df["quantity"] * df["price"]).round(2)

    print(f"Rows AFTER: {len(df)}")
    return df