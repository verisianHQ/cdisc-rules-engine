import os
from typing import Optional, Tuple

import pandas as pd

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
from cdisc_rules_engine.data_service.sql_data_preprocessor import (
    SqlDataPreprocessor,
)
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.models.dataset_metadata2 import VariableMetadata
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.standards.standards_factory import StandardsFactory
from cdisc_rules_engine.utilities.ig_specification import IGSpecification
from scripts.run_sql_validation import sql_run_single_rule_validation
from scripts.script_utils import get_library_metadata_from_cache

METADATA_CACHE = {}

TEST_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", DefaultFilePaths.CACHE.value)


def get_metadata(ig_specs: IGSpecification, define_xml_path: str):
    """
    Get metadata from cache or create it if not present.
    """
    key = f"{ig_specs['standard']}_{ig_specs['standard_version']}_{ig_specs['standard_substandard']}_{define_xml_path}"
    if key not in METADATA_CACHE:
        METADATA_CACHE[key] = get_library_metadata_from_cache(
            Validation_args(
                cache=TEST_CACHE_PATH,
                pool_size=None,
                dataset_paths=None,
                log_level=None,
                report_template=None,
                standard=ig_specs["standard"],
                version=ig_specs["standard_version"],
                substandard=ig_specs["standard_substandard"],
                controlled_terminology_package=set(),
                output=None,
                output_format=None,
                raw_report=None,
                define_version=None,
                external_dictionaries=None,
                rules=None,
                local_rules=None,
                custom_standard=None,
                progress=None,
                define_xml_path=define_xml_path,
                stf_file_path=None,
                validate_xml=None,
                sql_namespace=None,
            )
        )
    return METADATA_CACHE[key]


def _initialize_data_service(
    data_test_datasets: list,
    standards_context,
    use_pgserver: bool,
    data_service: Optional[PostgresQLDataService] = None,
) -> PostgresQLDataService:
    """Helper to handle persistent or new data service initialization."""
    if data_service:
        ds = data_service
        ds.pgi._drop_prefixed_tables()
        ds.datasets = []
        for test_ds in data_test_datasets:
            ds_metadata = standards_context.transform_dataset_metadata(
                SqlTestDatasetLoader.load_test_dataset(ds.pgi, test_ds)
            )
            ds.datasets.append(ds_metadata)
        SqlDataPreprocessor.run(ds, standards_context)
        return ds

    return PostgresQLDataService.from_list_of_testdatasets(
        data_test_datasets, standards_context, use_pgserver=use_pgserver, cache_path=TEST_CACHE_PATH
    )


def process_test_case_dataset_sql(
    regression_errors: dict,
    define_xml_file_path: str,
    data_test_datasets: list,
    ig_specs: IGSpecification,
    rule: dict,
    use_pgserver: bool = False,
    data_service: Optional[PostgresQLDataService] = None,
):
    metadata = get_metadata(ig_specs, define_xml_file_path)

    standards_context = StandardsFactory.get_standards_context(
        ig_specs.get("standard"),
        ig_specs.get("standard_version"),
        ig_specs.get("standard_substandard"),
        library_metadata=metadata,
    )
    ds = _initialize_data_service(data_test_datasets, standards_context, use_pgserver, data_service)
    regression_errors["datasets_import_sql"] = "SUCCESS"
    sql_results = sql_run_single_rule_validation(data_service=ds, rule=rule, standards_context=standards_context)
    regression_errors["results_present_sql"] = True
    sql_regression = extract_results_regression(sql_results)
    regression_errors["results_sql"] = sql_regression

    return sql_results, sql_regression


def extract_results_regression(results):
    res_regression = []

    if isinstance(results, dict):
        result_list = []
        for dataset_results in results.values():
            merged_by_dataset = {}
            for result in dataset_results:
                key = (result.get("dataset", ""), result.get("executionStatus", ""))
                if key in merged_by_dataset:
                    merged_by_dataset[key]["errors"].extend(result.get("errors", []))
                else:
                    merged = result.copy()
                    merged["errors"] = list(result.get("errors", []))
                    merged_by_dataset[key] = merged
            result_list.extend(merged_by_dataset.values())
    elif isinstance(results, list):
        result_list = results
    else:
        return res_regression

    for res in result_list:
        domain_res_regression = {
            "dataset": res.get("dataset", ""),
            "domain": res.get("domain", ""),
            "execution_status": res.get("executionStatus", ""),
            "execution_message": res.get("message", ""),
            "number_errors": len(res.get("errors", [])),
        }

        execution_status = res.get("executionStatus", "")
        errors = res.get("errors", [])

        if execution_status == "execution_error":
            domain_res_regression["errors"] = [
                {"error": error.get("error"), "message": error.get("message")}
                for error in sorted(errors, key=lambda x: x.get("message", ""))
            ]
        elif execution_status == "skipped":
            domain_res_regression["errors"] = []
        elif execution_status == "success":
            domain_res_regression["errors"] = [
                {
                    "row": error.get("row"),
                    "SEQ": error.get("SEQ"),
                    "USUBJID": error.get("USUBJID"),
                    "value": error.get("value"),
                }
                for error in sorted(errors, key=lambda x: x.get("row", 0))
            ]
        else:
            domain_res_regression["errors"] = [{"error": "unknown execution status"}]
        res_regression.append(domain_res_regression)
    return res_regression


def sharepoint_xlsx_to_test_datasets(path: str) -> list[TestDataset]:
    # Step 1: Read the "Datasets" sheet
    xlsx_data = pd.ExcelFile(path)
    datasets_df = pd.read_excel(xlsx_data, sheet_name="Datasets")

    # Step 2: Initialize list to store TestDataset objects
    test_datasets = []

    # Step 3: Iterate over each row in the "Datasets" sheet
    for _, row in datasets_df.iterrows():
        filename = row["Filename"]
        label = row["Label"]
        file_size = row.get("File Size")

        # Step 4: Read the sheet for the dataset
        if filename in xlsx_data.sheet_names:
            dataset_df = pd.read_excel(xlsx_data, sheet_name=filename, keep_default_na=False, na_values=[""])

            # Step 5: Extract variable details (name, label, type, length)
            variables, col_type_dict = extract_variables(dataset_df)

            # Step 6: Extract data (rest of the rows)
            data = extract_data(filename, col_type_dict, dataset_df)

            # Step 7: Create a TestDataset object and append it to the list
            test_datasets.append(
                TestDataset(
                    filename=filename,
                    name=filename.split(".")[0].upper(),
                    label=label,
                    variables=variables,
                    records=data,
                    file_size=None if pd.isna(file_size) else int(file_size),
                )
            )

    return test_datasets


def extract_variables(
    dataset_df: pd.DataFrame,
) -> Tuple[list[VariableMetadata], dict]:
    variables = []
    col_type_dict = {}
    for i, col in enumerate(dataset_df.columns):
        var_name = col  # Name from row 0
        if col.startswith("Unnamed:"):
            continue
        var_label = str(dataset_df[col].iloc[0])  # Label from row 1
        var_type = str(dataset_df[col].iloc[1])  # Type from row 2
        var_length = dataset_df[col].iloc[2]  # Length from row 3
        var_format = ""  # Format is always empty

        if var_type not in ["Char", "Num"]:
            raise ValueError(f"Unknown variable type: {var_type}. This data needs fixing.")

        # Create a variable dictionary
        variables.append(
            VariableMetadata(
                name=var_name, label=var_label, type=var_type, length=var_length, format=var_format, order=i + 1
            )
        )

        # collect appropriate column type for SQL
        col_type_dict[var_name] = var_type

    return variables, col_type_dict


def extract_data(filename: str, col_type_dict: dict, dataset_df: pd.DataFrame) -> dict:
    data = {}
    for col in dataset_df.columns:
        if col.startswith("Unnamed:"):
            continue
        column_name = col  # Column name from row 0
        column_values = dataset_df[col].iloc[3:].tolist()  # All values below row 3

        # Preprocess the column values based on the column type
        if col_type_dict[column_name].lower() == "num":
            try:
                column_values = [None if pd.isna(val) else float(val) for val in column_values]
            except ValueError as e:
                raise ValueError(f"Error converting column '{column_name}' in table '{filename}' to numeric: {e}")
        elif col_type_dict[column_name].lower() == "char":
            try:
                column_values = ["" if pd.isna(val) else str(val) for val in column_values]
            except ValueError as e:
                raise ValueError(f"Error converting column '{column_name}' in table '{filename}' to string: {e}")
        else:
            raise ValueError(f"Unsupported column type: {col_type_dict[column_name]} for rule")

        # Store the column name and its values in the data dictionary
        data[column_name] = column_values

    return data
