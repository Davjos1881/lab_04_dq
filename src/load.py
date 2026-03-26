from sqlalchemy import create_engine, text
import pandas as pd
import os

# Save to DW
def insert_ignore(df, table_name, engine):

    temp_table = f"tmp_{table_name}"

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    cols = ", ".join(df.columns)

    insert_sql = f"""
        INSERT IGNORE INTO {table_name} ({cols})
        SELECT {cols} FROM {temp_table};
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE {temp_table}"))


# Load to DW
def load_to_dw(dataframes):

    dim_date     = dataframes["dim_date"]
    dim_product  = dataframes["dim_product"]
    dim_customer = dataframes["dim_customer"]
    dim_location = dataframes["dim_location"]
    fact_sales   = dataframes["fact_sales"]

    # Conection
    engine = create_engine(
        "mysql+pymysql://root:@localhost:3306/lab04"
    )

    # Load dimensions
    insert_ignore(dim_date, "dim_date", engine)
    insert_ignore(dim_product, "dim_product", engine)
    insert_ignore(dim_customer, "dim_customer", engine)
    insert_ignore(dim_location, "dim_location", engine)


    # Avoid duplicates
    key_cols = [
        "invoice_date_key",
        "product_key",
        "customer_key",
        "location_key",
        "invoice_id"
    ]

    existing_keys_sql = f"""
        SELECT {', '.join(key_cols)}
        FROM fact_sales
    """

    try:
        existing_keys_df = pd.read_sql(existing_keys_sql, engine)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=key_cols)

    for col in key_cols:
        fact_sales[col] = fact_sales[col].astype(str)
        existing_keys_df[col] = existing_keys_df[col].astype(str)

    # Anti-join
    if not existing_keys_df.empty:
        merged = fact_sales.merge(
            existing_keys_df.drop_duplicates(),
            on=key_cols,
            how="left",
            indicator=True
        )

        fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        fact_new = fact_sales.copy()

    print(
        f"Fact total={len(fact_sales)} "
        f"new rows={len(fact_new)} "
        f"duplicates omitted={len(fact_sales) - len(fact_new)}"
    )

    # INSERT FACT
    if not fact_new.empty:
        insert_ignore(fact_new, "fact_sales", engine)
    else:
        print("No new rows for fact_sales")

    print("Load to Data Warehouse completed successfully")