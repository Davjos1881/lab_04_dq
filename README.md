**LAB-4. ETL Pipeline with Data Quality Validation (Great Expectations)**

By:

Jose David Santa Parra

Brayan Stiven Tigreros 

Valentina Morales

**Extract & Data Profiling**

Data Extraction

The pipeline begins by loading the raw transactional dataset into a Pandas DataFrame.

    def extract_raw_data(path):
        df_raw = pd.read_csv(path)
        return df_raw
    
Reads the original dataset from the data/raw/ directory
Preserve the raw data without any modifications

**Data Profiling (EDA)**

The data_profiling() function performs a systematic analysis covering all required aspects of the dataset.

1. Data Structure Overview

        df_raw.head()
        df_raw.shape
        df_raw.columns
   
Displays sample records
Reports number of rows and columns
Lists all available fields

2. Data Types and Memory Usage

        df_raw.dtypes
        df_raw.memory_usage(deep=True)

Validates column data types
Estimates memory consumption

3. Missing Values Analysis

        df_raw.isnull().sum()
        (df_raw.isnull().sum() / len(df_raw)) * 100

Counts null values per column
Computes percentage of missing data



4. Cardinality of Categorical Columns

        categorical_cols = ["product", "country", "invoice_date"]
        df_raw[col].nunique()

Measures the number of unique values
Helps detect inconsistencies (e.g., country variations)

5. Descriptive Statistics

        df_raw.describe()

Provides statistical summary of numeric columns
Helps detect anomalies such as negative values or outliers

6. Duplicate Detection

        df_raw['invoice_id'].value_counts()

Identifies repeated transaction IDs
Quantifies duplication issues affecting revenue

7. Revenue Consistency Check

        calc = df_raw["quantity"] * df_raw["price"]
        invalid = np.abs(df_raw["total_revenue"] - calc) > 0.01

Detects inconsistencies in derived revenue
Uses a tolerance of ±0.01

This supports the accuracy dimension.

8. Date Format Analysis

        formats = df_raw["invoice_date"].apply(detect_format)

Detected formats:

YYYY-MM-DD
YYYY/MM/DD
DD-MM-YYYY
Other / invalid

This highlights inconsistencies in temporal data representation.

9. Future Dates Detection

        future_dates = parsed_dates > pd.Timestamp("2023-12-31")

Identifies records outside the valid time range

10. Null-like Date Detection

        parsed.isnull().sum()

Captures malformed or non-standard date values

In addition to manual checks, a full profiling report is generated using ydata-profiling:

    profile = ProfileReport(df_raw, title="Lab 04 data profiling report")
    profile.to_file("reports/data_prof_report_lab04.html")

Output:

reports/data_prof_report_lab04.html

Contents:

  Column distributions
  
  Correlations
  
  Missing value patterns
  
  Data quality warnings

This dataset is then passed to:

Input validation (Great Expectations)
Cleaning
Transformation

**Input Data Validation**

Before any cleaning is applied, the pipeline performs a data quality assessment using Great Expectations.

The goal of this stage is not to fix data, but to:

Measure data quality issues
Establish a baseline quality score
Document failures through Data Docs

**Great Expectations Setup**

The validation is implemented in validate_input.py using a File Data Context:

    context = gx.get_context(mode="file", project_root_dir=str(inp_validate))

The GE configuration is stored in the project folder:

    great_expectations/
    This directory contains:
    great_expectations.yml → main config
    expectations/ → expectation suites (JSON)
    uncommitted/data_docs/ → generated reports

**Data Source and Batch Configuration**

The dataset is registered dynamically:

    data_source = context.data_sources.add_or_update_pandas(name="retail_etl_dataset")
    data_asset = data_source.add_dataframe_asset(name="retail_etl_dataset_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("retail_etl_dataset_batch")

Purpose:
Treats the Pandas DataFrame as a formal data asset
Enables reproducible validation runs
Keeps the pipeline modular

**Custom Accuracy Check**

Before validation, a helper column is created:

    df_raw["revenue_match"] = (
        abs(df_raw["total_revenue"] - df_raw["quantity"] * df_raw["price"]) <= 0.01
    )

GE does not natively support multi-column arithmetic checks easily.
So, this converts a business rule into a boolean validation

**Expectation Suite**

The Expectation Suite enforces rules across six data quality dimensions:

1. Completeness
customer_id NOT NULL
invoice_date NOT NULL

Ensures required fields are present.

2. Uniqueness
invoice_id UNIQUE

Prevents duplicate transactions affecting revenue.

3. Validity
quantity >= 1
price > 0
product IN catalog

Guarantees physically and logically valid values.

4. Accuracy
revenue_match == True

Ensures:

total_revenue ≈ quantity × price
5. Consistency
country IN {Colombia, Chile, Peru, Ecuador}

Standardizes categorical values.

6. Timeliness
invoice_date matches YYYY-MM-DD
invoice_date between 2023-01-01 and 2023-12-31

Ensures correct format and valid time range.

**Validation Execution**

    validation_results = batch.validate(suite)
    print(validation_results)
    
At this stage, failures are expected

**Data Docs Generation**

    context.build_data_docs()

This generates an HTML report which contains:

Pass/fail results per expectation
Validation statistics
Data quality insights

Key Design Principle

This stage does not modify data, it only measures quality.

**Data Cleaning**

The cleaning stage is responsible for removing or correcting invalid records that cannot safely proceed through the pipeline. According to the project requirements, cleaning must only be applied when strictly necessary and every action must be justified.

This step directly supports:

BO-1 (Financial Integrity) by eliminating incorrect revenue contributions
BO-2 (Reliable Trends) by ensuring valid temporal data
BO-3 (Product & Regional Insights) by preserving consistent records

**Cleaning Strategy Overview**

The following rules were applied in cleaning.py:

1. Removal of unnecessary columns

          if "revenue_match" in df.columns:
              df = df.drop(columns=["revenue_match"])

This column was auxiliary and not required for downstream processing.
Removing it simplifies the dataset and avoids redundancy.

2. Duplicate transactions removal

        df = df.drop_duplicates(subset=["invoice_id"], keep="first")

Issue: Duplicate invoice_id values inflate revenue.
Action: Keep the first occurrence and drop the rest.
Justification: Each invoice must be unique to ensure correct aggregation.

3. Invalid quantity and price filtering

        df = df[(df["quantity"] >= 1) & (df["price"] > 0)]

Issue: Negative or zero values are not physically meaningful.
Action: Remove invalid rows.
Justification: Prevents corruption of revenue calculations.

4. Handling null-like date values

        df["invoice_date"] = df["invoice_date"].replace(["N/A", "NULL", "", "nan"], pd.NA)
        df = df.dropna(subset=["invoice_date"])

Issue: Dates stored as strings like "N/A" or "NULL".
Action: Convert to nulls and remove affected rows.
Justification: Dates are critical for time-based analysis.

5. Removal of future dates

        temp_dates = pd.to_datetime(df["invoice_date"], errors="coerce")
        df = df[temp_dates <= "2023-12-31"]

Issue: Dates outside the valid range (beyond 2023).
Action: Filter out future records.
Justification: Ensures consistency with business constraints.

6. Handling missing customer IDs

        null_mask = df["customer_id"].isnull()
        max_id = int(df["customer_id"].max())
        df.loc[null_mask, "customer_id"] = range(max_id + 1, max_id + 1 + null_mask.sum())

Issue: Missing customer_id values.
Action: Assign new unique IDs.
Justification: Preserves records instead of dropping them, enabling customer-level analysis.

7. Revenue correction

        bad_revenue = abs(df["total_revenue"] - df["quantity"] * df["price"]) > 0.01
        df.loc[bad_revenue, "total_revenue"] = (df["quantity"] * df["price"]).round(2)

Issue: Inconsistent revenue calculations.
Action: Recalculate using quantity × price.
Justification: Guarantees financial accuracy.

**Logging and Traceability**

The script prints a full audit of the cleaning process:

        print(f"Rows BEFORE: {len(df)}")
        print(f"Duplicates removed: ...")
        print(f"Negative quantity/price removed: ...")
        print(f"Null-like dates removed: ...")
        print(f"Future dates removed: ...")
        print(f"Rows AFTER: {len(df)}")

This ensures:

Transparency of data loss
Reproducibility
Alignment with BO-4 (Defensible Reporting)

Output:

The cleaned dataset is saved as:

    data/processed/retail_clean.csv

This dataset is guaranteed to:

Contain only valid transactions
Be free of duplicates
Have consistent and reliable revenue values
Be ready for transformation

**Transformation**

Unlike cleaning, this step:

Does not remove records
Applies transformations to all valid rows
Prepares the data for the star schema and business analysis

**Transformation Logic**

Implemented in transform.py:

    def transform_data(df_clean):
        df = df_clean.copy()

A copy is created to preserve the integrity of the cleaned dataset.

1. Country Standardization

        country_map = {
            "Colombia": "CO", "colombia": "CO", "CO": "CO",
            "Ecuador": "EC", "ecuador": "EC", "EC": "EC",
            "Peru": "PE", "peru": "PE", "PE": "PE",
            "Chile": "CL", "chile": "CL", "CL": "CL"
        }
        df["country"] = df["country"].replace(country_map)

Normalize inconsistent country formats into ISO 3166-1 alpha-2 codes an ensures consistent grouping in analytics and reporting

2. Date Parsing and Feature Extraction

        df["invoice_date"] = pd.to_datetime(df["invoice_date"], dayfirst=True, errors="coerce")
        df["year"]         = df["invoice_date"].dt.year
        df["month"]        = df["invoice_date"].dt.month
        df["day"]          = df["invoice_date"].dt.day
        df["day_of_week"]  = df["invoice_date"].dt.day_name()

Converts dates into a proper datetime format
Extract time-based features

3. Customer ID Type Standardization

        df["customer_id"] = df["customer_id"].astype("Int64")

Convert to nullable integer type and it ensures compatibility with the dimension tables and foreign key relationships in the data warehouse.

4. Product Name Normalization

        df["product"] = df["product"].str.strip().str.title()

Remove whitespace inconsistencies.
Standardize casing, preventing duplicate categories such as:
"laptop" vs "Laptop"

5. Revenue Segmentation

        df["revenue_bin"] = pd.qcut(
            df["total_revenue"],
            q=3,
            labels=["Low", "Medium", "High"]
        )

Categorize transactions into revenue tiers

Low → bottom 33%

Medium → middle 33%

High → top 33%

The transformed dataset is saved as:

    data/processed/retail_transformed.csv

**Post-Transformation Validation (Great Expectations)**

After the transformation stage, the pipeline performs a second validation using Great Expectations.

At this point, validation is no longer exploratory.

**Great Expectations Setup**

The same GE project structure is reused:

  context = gx.get_context(mode="file", project_root_dir=str(inp_validate))

A new datasource and asset are registered:

    data_source = context.data_sources.add_or_update_pandas(name="retail_etl_dataset_clean")
    data_asset = data_source.add_dataframe_asset(name="retail_etl_dataset_clean_asset")

The transformed DataFrame is then passed as a batch for validation.

    Accuracy Check (Recomputed)
    df_transformed["revenue_match"] = (
        abs(df_transformed["total_revenue"] - df_transformed["quantity"] * df_transformed["price"]) <= 0.01
    )

This ensures that revenue consistency is validated again after all transformations.

**Expectation Suite**

A new suite is defined:

suite = gx.ExpectationSuite(name="retail_etl_clean_suite")

Unlike the input stage, all expectations must now pass.

Validation Rules
Completeness
customer_id NOT NULL
invoice_date NOT NULL

All missing values must have been resolved during cleaning.

Uniqueness
invoice_id UNIQUE

Duplicate transactions must have been removed.

Validity
quantity >= 1
price > 0
total_revenue > 0
product IN catalog

Ensures all numeric and categorical values are valid after cleaning.

Accuracy
revenue_match == True

Guarantees:

total_revenue ≈ quantity × price
Consistency
country IN {CO, EC, PE, CL}

Validates the transformation to ISO country codes.

Timeliness
invoice_date is datetime
invoice_date between 2023-01-01 and 2023-12-31

Ensures:

Proper type conversion
Valid temporal range
Transformation-Specific Validations
month BETWEEN 1 AND 12
month NOT NULL
day_of_week NOT NULL
revenue_bin IN {Low, Medium, High}

These validate the newly engineered features.

The pipeline computes a final Data Quality (DQ) score:
    
    stats = validation_results["statistics"]
    score = stats["success_percent"]

Output Example

    DQ Score (output): 100.0%
    Passed: 15 / 15

**Dimensional Modeling (Star Schema)**


After validation, the pipeline transforms the dataset into a star schema.

A star schema separates data into a fact table and dimension tables:

fact_sales
dim_date
dim_product
dim_customer
dim_location

Each record in fact_sales represents one transaction (one invoice, one product, one customer, one date)

**Load (Data Warehouse)**

The final stage of the pipeline loads the dimensional model into a relational database.

This implementation uses MySQL as the data warehouse and loads:

Dimension tables first

Fact table last

**Database Connection**

    engine = create_engine(
        "mysql+pymysql://root:@localhost:3306/lab04"
    )

We used SQLAlchemy for database interaction, so:

A local MySQL instance is required with a database named lab04

**Insert Strategy**

A custom function is used to handle inserts:

    def insert_ignore(df, table_name, engine):


Load data into a temporary table:

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

Insert into target table:

    INSERT IGNORE INTO table_name (...)
    SELECT ... FROM temp_table;

INSERT IGNORE prevents duplicate key errors

Loading Order:

    insert_ignore(dim_date, "dim_date", engine)
    insert_ignore(dim_product, "dim_product", engine)
    insert_ignore(dim_customer, "dim_customer", engine)
    insert_ignore(dim_location, "dim_location", engine)

Then:

    insert_ignore(fact_new, "fact_sales", engine)

Dimensions must exist before facts. Fact table contains foreign keys referencing dimensions

**Duplicate Handling (Fact Table)**

Before inserting into fact_sales:

Step 1: Retrieve existing keys

    SELECT invoice_date_key, product_key, customer_key, location_key, invoice_id
    FROM fact_sales

Step 2: Normalize types

    fact_sales[col] = fact_sales[col].astype(str)
    existing_keys_df[col] = existing_keys_df[col].astype(str)

Ensures consistent comparison.

Step 3: Anti-join

    merged = fact_sales.merge(
        existing_keys_df,
        on=key_cols,
        how="left",
        indicator=True
    )

Filter new records:
    
    fact_new = merged[merged["_merge"] == "left_only"]
    
Final Insert

    if not fact_new.empty:
        insert_ignore(fact_new, "fact_sales", engine)

**Pipeline Orchestration (main.py)**

The entire ETL pipeline is orchestrated through main.py.

Raw Data → Extract & Profiling → Input Validation → Cleaning → Transformation → Post-Validation → Dimensional Modeling → Load


**Execution Flow**

1. Extraction and Profiling

        df_raw = extract_raw_data(raw_file)
        data_profiling(df_raw)

Loads raw dataset

Performs exploratory data analysis (EDA)

Identifies initial data quality issues

2. Input Validation (Great Expectations)

        input_data_validation(df_raw, inp_validate)

Evaluates raw data quality

Generates Data Docs

Establishes baseline DQ metrics

3. Cleaning

        df_clean = clean_data(df_raw)
        df_clean.to_csv(clean_file, index=False)

Removes invalid records

Fixes inconsistencies

Saves cleaned dataset

4. Transformation

        df_transformed = transform_data(df_clean)
        df_transformed.to_csv(transformed_file, index=False)

Standardizes and enriches data

Creates analytical features

5. Post-Transformation Validation

        output_data_validation(df_transformed, inp_validate)

Enforces data quality rules

Ensures all expectations pass

6. Dimensional Modeling

        df_dimensional_model = transform_data_DM(df_transformed)

Builds star schema

Intermediate results are stored for inspection:

        tabla.to_csv(dims_test / f"{nombre}.csv", index=False)

7. Load to Data Warehouse

        load_to_dw(df_dimensional_model)

Loads all tables into MySQL

Applies duplicate control

Ensures referential integrity

8. Logging

Each stage is tracked using:

        log_progress("message", log_file)

**How to Run the Project**

1. Clone de repository

        git clone https://github.com/Davjos1881/lab_04_dq

2. Create and activate a Virtual Environment

Create it

    python -m venv venv

Activate it

    venv\Scripts\activate

3.  Install Dependencies

        pip install -r requirements.txt
    
4. Configure the Project Paths

The project uses local file paths in main.py. Update base_path so it points to your local repository folder:

    base_path = Path(r"YourPath")

Yoy may also check in the extract.py file and change the patch where the report is saved

    profile.to_file(r"YourPath\reports\data_prof_report_lab04.html")  

5. Configure the Database Connection

The load stage writes the final star schema into a local MySQL database.

In load.py, update the SQLAlchemy connection string

    mysql+pymysql://root:@localhost:3306/lab04

6. Run the ETL Pipeline

Execute the main orchestration script:

    python src/main.py

This runs the pipeline in order:

Raw Data → Extract & Data Profiling → Input Validation → Cleaning → Transformation → Post-Transformation Validation → Dimensional Modeling → Load into the Data Warehouse

7. Review the Outputs

After the pipeline finishes, review these generated artifacts:

Processed data:

    data/processed/retail_clean.csv
    data/processed/retail_transformed.csv

Dimensional model exports for inspection:

    data/dims_test/
    
    Great Expectations reports:
    
    great_expectations/uncommitted/data_docs/

Logs:

    logs/log_file.txt

If the pipeline is configured correctly, the final tables should also be loaded into the MySQL database.

8. Business Intelligence in Power BI

The repository includes the Power BI document used for business analysis.

Like in most of our projects, the report connects to the MySQL Data Warehouse through an ODBC DSN, so Power BI reads directly from the modeled tables.

To open the report:

Make sure the MySQL ODBC driver is available

Make sure the DSN is configured correctly

Open the Power BI file included in the repository

Refresh the data connection

Example of how the dashboard should look:

<img width="1310" height="737" alt="image" src="https://github.com/user-attachments/assets/339b56c3-a37c-4e44-99eb-3aff2d329f23" />
