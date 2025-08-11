import os
import pandas as pd
from unittest.mock import patch

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.test_dataset import TestDataset
from scripts.run_sql_validation import sql_run_single_rule_validation
from scripts.run_validation import run_single_rule_validation


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_regression(mock_get_dataset_class, pytestconfig, get_core_rules_df, get_core_rule):
    mock_get_dataset_class.return_value = None
    regression_df = get_core_rules_df()

    local_path = "/Users/verisian/data/CORE/CDISC_Sharepoint_dump_20250806/"

    # set up SQL Engine
    ig_specs = {
        "standard": "SDTMIG",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }

    # regression fields
    regression_df["core_id_is_null"] = False
    regression_df["has_core_structure"] = True
    regression_df["in_cache"] = True
    regression_df["negative_folder"] = ""
    regression_df["positive_folder"] = ""
    regression_df["negative_folder_data"] = ""
    regression_df["positive_folder_data"] = ""
    regression_df["negative_datasets_worksheet"] = None
    regression_df["positive_datasets_worksheet"] = None

    # tests
    regression_df["core_execute"] = False
    regression_df["core_unit_test_pass"] = False
    regression_df["core_time"] = 0
    regression_df["sql_core_execute"] = False
    regression_df["sql_core_unit_test_pass"] = False
    regression_df["sql_core_time"] = 0
    regression_df["sql_core_time_delta"] = 0

    for idx, row in regression_df.iterrows():
        cur_core_id = str(row["Core-ID"])
        if cur_core_id:
            if cur_core_id.startswith("CORE-"):
                rule = get_core_rule(cur_core_id)
                if rule:
                    # get data for the rule
                    rule_ids = row["rids"]
                    print("on rule: " + str(rule_ids))
                    # find data if present:
                    for rid in rule_ids:
                        paths = get_paths(local_path, row, rid)
                        if len(paths) == 1:
                            p = paths[0]
                            negative_folder = p + "/negative/"
                            positive_folder = p + "/positive/"
                            max_neg_folder = find_max_dir(negative_folder)
                            max_pos_folder = find_max_dir(positive_folder)
                            regression_df.at[idx, "negative_folder"] = "/".join(max_neg_folder.split("/")[-5:])
                            regression_df.at[idx, "positive_folder"] = "/".join(max_pos_folder.split("/")[-5:])
                            negative_file = find_data_file(max_neg_folder + "/data")
                            positive_file = find_data_file(max_pos_folder + "/data")
                            regression_df.at[idx, "negative_folder_data"] = "/".join(negative_file.split("/")[-7:])
                            regression_df.at[idx, "positive_folder_data"] = "/".join(positive_file.split("/")[-7:])

                            if positive_file:
                                # execute rule in old engine
                                try:
                                    positive_test_datasets = sharepoint_xlsx_to_test_datasets(positive_file)
                                    regression_df.at[idx, "positive_datasets_worksheet"] = True
                                except ValueError as e:
                                    if str(e) == "Worksheet named 'Datasets' not found":
                                        regression_df.at[idx, "positive_datasets_worksheet"] = False
                                    else:
                                        # Re-raise the error if it's not the one we're specifically catching
                                        raise

                                old_results = run_single_rule_validation(
                                    positive_test_datasets,
                                    rule,
                                    standard=ig_specs["standard"],
                                    standard_version=ig_specs["standard_version"],
                                )
                                print(old_results)
                                # execute rule in SQL engine
                                # ds = PostgresQLDataService.from_list_of_testdatasets(positive_test_datasets, ig_specs)
                                # sql_results = sql_run_single_rule_validation(data_service=ds, rule=rule)
                            if negative_file:
                                try:
                                    negative_test_datasets = sharepoint_xlsx_to_test_datasets(negative_file)
                                    regression_df.at[idx, "negative_datasets_worksheet"] = True
                                except ValueError as e:
                                    if str(e) == "Worksheet named 'Datasets' not found":
                                        regression_df.at[idx, "negative_datasets_worksheet"] = False
                                    else:
                                        raise
                                # execute rule in old engine
                                old_results = run_single_rule_validation(
                                    negative_test_datasets,
                                    rule,
                                    standard=ig_specs["standard"],
                                    standard_version=ig_specs["standard_version"],
                                )
                                print(old_results)
                                # execute rule in SQL engine
                                # ds = PostgresQLDataService.from_list_of_testdatasets(negative_test_datasets, ig_specs)
                                # sql_results = sql_run_single_rule_validation(data_service=ds, rule=get_sample_lb_rule)
                        else:
                            print(f"Found multiple paths for {rid}: {paths}")

                    assert rule is not None
                else:
                    regression_df.at[idx, "in_cache"] = False
            else:
                regression_df.at[idx, "has_core_structure"] = False
                regression_df.at[idx, "in_cache"] = False
        else:
            regression_df["core_id_is_null"] = True
            regression_df.at[idx, "has_core_structure"] = False
            regression_df.at[idx, "in_cache"] = False

    # data = validate_single_rule(datasets, rule)

    rule = get_core_rule("CORE-000254")
    assert rule is not None

    output_df = regression_df.drop(columns=["Description", "Standard Version", "Scope", "std", "rids"], errors="ignore")
    output_df.to_json(
        str(pytestconfig.rootpath) + "/tests/resources/rules/rules.json",
        orient="records",
        indent=2,
    )


def get_paths(local_path: str, row: pd.Series, rid: str) -> list[str]:
    paths = []
    if "SDTMIG" in row["std"]:
        paths.extend(
            find_dirs(
                local_path + "unitTesting/SDTMIG",
                rid,
                case_insensitive=True,
            )
        )
    wanted = {"ADAMIG", "ADaMIG", "ADaMIG-MD", "ADTTE"}
    if any(s in wanted for s in row["std"]):
        paths.extend(
            find_dirs(
                local_path + "unitTesting/ADAMIG",
                rid,
                case_insensitive=True,
            )
        )
    paths.extend(
        find_dirs(
            local_path + "unitTesting/FDA Business Rules",
            rid,
            case_insensitive=True,
        )
    )
    paths.extend(
        find_dirs(
            local_path + "unitTesting/FDA Validator Rules",
            rid,
            case_insensitive=True,
        )
    )
    return paths


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

        # Step 4: Read the sheet for the dataset
        if filename in xlsx_data.sheet_names:
            dataset_df = pd.read_excel(xlsx_data, sheet_name=filename)

            # Step 5: Extract variable details (name, label, type, length)
            variables = []
            for col in dataset_df.columns:
                var_name = col  # Name from row 0
                var_label = dataset_df[col].iloc[0]  # Label from row 1
                var_type = dataset_df[col].iloc[1]  # Type from row 2
                var_length = dataset_df[col].iloc[2]  # Length from row 3
                var_format = ""  # Format is always empty

                # Create a variable dictionary
                variables.append(
                    {"name": var_name, "label": var_label, "type": var_type, "length": var_length, "format": var_format}
                )

            # Step 6: Extract data (rest of the rows)
            data = {}
            for col in dataset_df.columns:
                column_name = col  # Column name from row 0
                column_values = dataset_df[col].iloc[4:].tolist()  # All values below row 3

                # Store the column name and its values in the data dictionary
                data[column_name] = column_values

            # Step 7: Create a TestDataset object and append it to the list
            test_datasets.append(
                TestDataset(
                    filename=filename,
                    filepath=filename,
                    name=filename.split(".")[0],
                    label=label,
                    variables=variables,
                    records=data,
                )
            )

    return test_datasets


def find_dirs(root, target_name, case_insensitive=False) -> list[str]:
    matches = []
    for d in os.listdir(root):
        if (d == target_name) or (case_insensitive and d.lower() == target_name.lower()):
            matches.append(os.path.join(root, d))
    return matches


def find_max_dir(root) -> str:
    max = 0
    max_d = ""
    try:
        for d in os.listdir(root):
            if d.isdigit():
                d_int = int(d)
                if d_int >= max:
                    max = d_int
                    max_d = os.path.join(root, d)
        return max_d
    except FileNotFoundError:
        return ""


def find_data_file(path: str) -> str:
    if not path:
        return ""
    accepted_extensions = ["xls", "xlsx"]
    try:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            extension = filename.split(".")[-1].lower()
            if os.path.isfile(full_path) and extension in accepted_extensions:
                return path + "/" + filename
    except FileNotFoundError:
        return ""
    return ""


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_existing_rule(mock_get_dataset_class, get_sample_lb_rule, get_sample_lb_dataset):
    mock_get_dataset_class.return_value = None
    ig_specs = {
        "standard": "SDTMIG",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], ig_specs)
    data = sql_run_single_rule_validation(data_service=ds, rule=get_sample_lb_rule)

    assert "LB" in data
    assert len(data["LB"]) == 1
    assert data["LB"][0]["message"] == "LBSEQ greater than 0"
    assert len(data["LB"][0]["errors"]) == 2
