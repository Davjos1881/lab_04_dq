import great_expectations as gx
import pandas as pd

def output_data_validation(df_transformed, inp_validate):

    context = gx.get_context(mode="file", project_root_dir=str(inp_validate))

    # Reuse same datasource
    data_source = context.data_sources.add_or_update_pandas(name="retail_etl_dataset_clean")
    data_asset = data_source.add_dataframe_asset(name="retail_etl_dataset_clean_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("retail_etl_dataset_clean_batch")

    # Accuracy helper column
    df_transformed["revenue_match"] = (
        abs(df_transformed["total_revenue"] - df_transformed["quantity"] * df_transformed["price"]) <= 0.01
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df_transformed})

    suite = gx.ExpectationSuite(name="retail_etl_clean_suite")

    # Completeness — must pass now
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date"))

    # Uniqueness — must pass now
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id"))

    # Validity — must pass now
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_revenue", min_value=0.01))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="product",
        value_set=["Headphones", "Keyboard", "Mouse", "Phone", "Laptop", "Printer", "Monitor", "Tablet"]
    ))

    # Accuracy — must pass now
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="revenue_match", value_set=[True]
    ))

    # Consistency — must pass now with ISO 3166-1
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="country", value_set=["CO", "EC", "PE", "CL"]
    ))

    # Timeliness — must pass now
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="invoice_date", regex=r"^\d{4}-\d{2}-\d{2}"
    ))

    # New transformation columns
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="revenue_bin", value_set=["Low", "Medium", "High"]
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="month"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="day_of_week"))

    context.suites.add_or_update(suite)

    validation_results = batch.validate(suite)

    # Print DQ Score
    stats = validation_results["statistics"]
    score = stats["success_percent"]
    print(f"\nDQ Score (output): {score:.1f}%")
    print(f"Passed: {stats['successful_expectations']} / {stats['evaluated_expectations']}")

    context.build_data_docs()