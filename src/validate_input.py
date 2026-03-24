from multiprocessing import context
import great_expectations as gx
import pandas as pd


def input_data_validation(df_raw, inp_validate):
    
    # Get the Ephemeral Data Context
    context = gx.get_context(mode="file", project_root_dir=str(inp_validate))

    # Add a Pandas Data Source
    data_source_name = "retail_etl_dataset"
    data_source = context.data_sources.add_or_update_pandas(name=data_source_name)

    # Add a Data Asset to the Data Source
    data_asset_name = "retail_etl_dataset_asset"
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    # Define the Batch Definition name
    batch_definition_name = "retail_etl_dataset_batch"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

    # Accuracy - columna auxiliar sobre df_raw
    df_raw["revenue_match"] = (
        abs(df_raw["total_revenue"] - df_raw["quantity"] * df_raw["price"]) <= 0.01
    )

    # Pasar df_raw al batch
    batch_parameters = {"dataframe": df_raw}
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Create an Expectation Suite
    expectation_suite_name = "retail_etl_dataset_suite"
    suite = gx.ExpectationSuite(name=expectation_suite_name)

    # Completeness
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="invoice_date")
    )

    # Uniqueness
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="invoice_id")
    )

    # Validity
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="product", value_set=["Headphones", "Keyboard", "Mouse", "Phone", "Laptop", "Printer", "Monitor", "Tablet"])
    )

    # Accuracy
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="revenue_match",
            value_set=[True]
        )
    )

    # Consistency
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="country", value_set=["Colombia", "Chile", "Peru", "Ecuador"])
    )

    # Timeliness
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(column="invoice_date", regex=r"^\d{4}-\d{2}-\d{2}$")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="invoice_date", min_value="2023-01-01", max_value="2023-12-31")
    )

    # Add the Expectation Suite to the Context
    context.suites.add_or_update(suite)
    # Validate
    validation_results = batch.validate(suite)
    print(validation_results)

    # Generar Data Docs
    context.build_data_docs()
    context.open_data_docs()
    