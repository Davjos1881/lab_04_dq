import pandas as pd

# Dim date
def create_dim_date(df):

    dim_date = df[["invoice_date"]].drop_duplicates().reset_index(drop=True)

    dim_date = dim_date[dim_date["invoice_date"].notna()]

    dim_date["invoice_date"] = pd.to_datetime(dim_date["invoice_date"])

    dim_date["invoice_date_key"]  = dim_date.index + 1

    dim_date["full_date"] = dim_date["invoice_date"]
    dim_date["year"]      = dim_date["invoice_date"].dt.year
    dim_date["month"]     = dim_date["invoice_date"].dt.month
    dim_date["day"]       = dim_date["invoice_date"].dt.day
    dim_date["day_name"]  = dim_date["invoice_date"].dt.day_name()

    dim_date = dim_date[["invoice_date_key", "full_date", "year", "month", "day", "day_name"]]

    return dim_date


# Dim product
def create_dim_product(df):

    dim_product = df[["product", "price"]].drop_duplicates().reset_index(drop=True)

    dim_product["product_key"] = dim_product.index + 1

    dim_product = dim_product[["product_key", "product", "price"]]

    return dim_product


# Dim customer
def create_dim_customer(df):

    dim_customer = df[["customer_id"]].drop_duplicates().reset_index(drop=True)

    dim_customer["customer_key"]  = dim_customer.index + 1

    dim_customer = dim_customer[["customer_key", "customer_id"]]

    return dim_customer


# Dim location
def create_dim_location(df):

    dim_location = df[["country"]].drop_duplicates().reset_index(drop=True)

    dim_location["location_key"] = dim_location.index + 1

    dim_location = dim_location[["location_key", "country"]]

    return dim_location


# Fact table
def create_fact_sales(df, dim_product, dim_customer, dim_location, dim_date):

    df["invoice_date"] = pd.to_datetime(df["invoice_date"])

    # Merge para traer los keys
    df = df.merge(dim_product, on=["product", "price"], how="left")
    df = df.merge(dim_location, on="country",     how="left")
    df = df.merge(dim_customer, on="customer_id", how="left")

    # Para dim_date el merge debe ser por full_date
    dim_date_merge = dim_date[["invoice_date_key", "full_date"]].copy()
    dim_date_merge["full_date"] = pd.to_datetime(dim_date_merge["full_date"])
    df = df.merge(dim_date_merge, left_on="invoice_date", right_on="full_date", how="left")

    fact_sales = df[[
        "invoice_id",
        "invoice_date_key",
        "location_key",
        "customer_key",
        "product_key",
        "quantity",
        "total_revenue",
        "revenue_bin"
    ]].copy()

    return fact_sales

# Transform
def transform_data_DM(df):

    dim_date     = create_dim_date(df)
    dim_product  = create_dim_product(df)
    dim_customer = create_dim_customer(df)
    dim_location = create_dim_location(df)

    fact_sales = create_fact_sales(
        df,
        dim_product,
        dim_customer,
        dim_location,
        dim_date
    )

    return {
        "dim_date":     dim_date,
        "dim_product":  dim_product,
        "dim_customer": dim_customer,
        "dim_location": dim_location,
        "fact_sales":   fact_sales
    }