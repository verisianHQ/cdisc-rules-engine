import pandas as pd
import os
import re

import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

XLSX_FILE_EXTENSIONS = [".xlsx"]

# warnings.filterwarnings("ignore")


def validate_folder_structure(
    rtype: str,
    root_folder: str,
    rule_id: str,
    error_messages: list[str],
    missing_lib_sheets: list[str],
    missing_ds_sheets: list[str],
) -> list[str]:
    # Define the expected folder structure
    pos_neg_folder_names = ["positive", "negative", "skipped"]
    data_results_folder_names = ["data", "results"]

    # Check if root folder exists
    if not os.path.exists(root_folder):
        error_messages.append(f"{rtype}:{rule_id}: root folder is missing: <{root_folder}>.")

    validate_rule_id_folder(error_messages, root_folder, pos_neg_folder_names, rtype, rule_id)

    # Iterate over required subfolders (positive, negative)
    for pos_neg_folder in pos_neg_folder_names:
        pos_neg_folder_path = os.path.join(root_folder, pos_neg_folder)

        if not os.path.exists(pos_neg_folder_path):
            if pos_neg_folder != "skipped":
                error_messages.append(f"{rtype}:{rule_id}: required folder is missing: <{pos_neg_folder}>.")
            continue

        validate_pos_neg_folder(error_messages, pos_neg_folder_path, pos_neg_folder, rtype, rule_id)

        # List subdirectories in 'positive' or 'negative'
        maybe_two_digit_folders = [f.name for f in os.scandir(pos_neg_folder_path) if f.is_dir()]

        validate_two_digit_folders(
            error_messages,
            missing_lib_sheets,
            missing_ds_sheets,
            maybe_two_digit_folders,
            pos_neg_folder_path,
            data_results_folder_names,
            pos_neg_folder,
            rtype,
            rule_id,
        )

    return error_messages


def validate_rule_id_folder(
    error_messages: list, root_folder: str, pos_neg_folder_names: str, rtype: str, rule_id: str
):
    # Ensure no files in ruleid folder level
    for file in os.scandir(root_folder):
        if file.is_file() and not file.name.startswith("."):
            error_messages.append(f"{rtype}:{rule_id}: File found in rule root folder: <{file.name}>.")

    for f in os.scandir(root_folder):
        if f.is_dir() and f.name not in ["dev", "archive", *pos_neg_folder_names]:
            error_messages.append(f"{rtype}:{rule_id}: Unexpected folder in root folder: <'{f.name}'>.")


def validate_pos_neg_folder(
    error_messages: list, pos_neg_folder_path: str, pos_neg_folder: str, rtype: str, rule_id: str
):
    # Ensure no files in positive/negative folder level directly
    for file in os.scandir(pos_neg_folder_path):
        if file.is_file() and not file.name.startswith("."):
            error_messages.append(f"{rtype}:{rule_id}: Unexpected file found in <{pos_neg_folder}>: <{file.name}>.")


def validate_two_digit_folders(
    error_messages: list[str],
    missing_lib_sheets: list[str],
    missing_ds_sheets: list[str],
    maybe_two_digit_folders: list,
    pos_neg_folder_path: str,
    data_results_folder_names: list,
    pos_neg_folder: str,
    rtype: str,
    rule_id: str,
):
    if "01" not in maybe_two_digit_folders:
        error_messages.append(f"{rtype}:{rule_id}: required folder is missing: <{pos_neg_folder}/01>")

    two_digit_regex = re.compile(r"^\d{2}$")
    for maybe_two_digit_folder in maybe_two_digit_folders:
        maybe_two_digit_folder_path = os.path.join(pos_neg_folder_path, maybe_two_digit_folder)

        # Check all subfolders for two-digit naming
        if not two_digit_regex.match(maybe_two_digit_folder):
            error_messages.append(
                f"{rtype}:{rule_id}: test case folder does not match two-digit naming: "
                f"<{pos_neg_folder}/{maybe_two_digit_folder}>."
            )
            continue

        two_digit_folder_path = maybe_two_digit_folder_path
        two_digit_folder = maybe_two_digit_folder

        # Check that there are no unexpected folders at this level
        for f in os.scandir(two_digit_folder_path):
            if f.is_dir() and f.name not in data_results_folder_names:
                error_messages.append(
                    f"{rtype}:{rule_id}: Unexpected folder found: <{pos_neg_folder}/{two_digit_folder}/{f.name}>"
                )

        for subfolder_name in data_results_folder_names:
            # Check for 'data' and 'results' subfolders
            subfolder_data_results_path = os.path.join(two_digit_folder_path, subfolder_name)
            if not os.path.exists(subfolder_data_results_path):
                error_messages.append(
                    f"{rtype}:{rule_id}: Subfolder missing: <{pos_neg_folder}/{two_digit_folder}/{subfolder_name}>."
                )
                continue

            # Check if there is at least one '.xlsx' file in the 'data' folder
            json_extension = ".json"
            if subfolder_name == "data":
                data_folder_validation(
                    error_messages,
                    missing_lib_sheets,
                    missing_ds_sheets,
                    subfolder_data_results_path,
                    two_digit_folder,
                    rtype,
                    rule_id,
                    pos_neg_folder,
                )

            # Check if there are any non-.json or non-xlsx files in results
            if subfolder_name == "results":
                results_folder_validation(
                    error_messages,
                    json_extension,
                    subfolder_data_results_path,
                    two_digit_folder,
                    rtype,
                    rule_id,
                    pos_neg_folder,
                )


def data_folder_validation(
    error_messages: list[str],
    missing_lib_sheets: list[str],
    missing_ds_sheets: list[str],
    subfolder_data_results_path: str,
    two_digit_folder: str,
    rtype: str,
    rule_id: str,
    pos_neg_folder: str,
):
    xlsx_files_in_folder = [f for f in os.scandir(subfolder_data_results_path) if f.is_file() and is_xlsx_file(f.name)]
    found_xlsx = any(xlsx_files_in_folder)
    if not found_xlsx:
        error_messages.append(
            f"{rtype}:{rule_id}: Missing '.xlsx' file in: <{pos_neg_folder}/{two_digit_folder}/data>."
        )
    if len(xlsx_files_in_folder) > 1:
        error_messages.append(
            f"{rtype}:{rule_id}: Multiple '.xlsx' files in: <{pos_neg_folder}/{two_digit_folder}/data>."
        )

    for f in xlsx_files_in_folder:
        validate_xlsx_file(
            error_messages, missing_lib_sheets, missing_ds_sheets, f, rtype, rule_id, pos_neg_folder, two_digit_folder
        )

    for file in os.scandir(subfolder_data_results_path):
        if file.is_file() and not (is_xlsx_file(file.name) or is_xml_file(file.name)):
            error_messages.append(
                f"{rtype}:{rule_id}: Non-xlsx and non-xml file found in: " f"<{pos_neg_folder}/{two_digit_folder}/data>"
            )

    for f in os.scandir(subfolder_data_results_path):
        if f.is_dir():
            error_messages.append(
                f"{rtype}:{rule_id}: Unexpected folder found: <{pos_neg_folder}/{two_digit_folder}/data/{f.name}>"
            )


def validate_xlsx_file(
    error_messages: list,
    missing_lib_sheets: list,
    missing_ds_sheets: list,
    f: os.DirEntry,
    rtype: str,
    rule_id: str,
    pos_neg_folder: str,
    two_digit_folder: str,
):
    try:
        xlsx_data = pd.ExcelFile(f)
        try:
            pd.read_excel(xlsx_data, sheet_name="Library")
        except ValueError:
            error_messages.append(
                f"{rtype}:{rule_id}: Missing Library sheet in: " f"<{pos_neg_folder}/{two_digit_folder}/data/{f.name}>"
            )
            missing_lib_sheets.append(f"{rule_id}/{pos_neg_folder}/{two_digit_folder}/data/{f.name}")
        try:
            pd.read_excel(xlsx_data, sheet_name="Datasets")
        except ValueError:
            error_messages.append(
                f"{rtype}:{rule_id}: Missing Datasets sheet in: " f"<{pos_neg_folder}/{two_digit_folder}/data/{f.name}>"
            )
            missing_ds_sheets.append(f"{rule_id}/{pos_neg_folder}/{two_digit_folder}/data/{f.name}")
    except TypeError as te:
        if "extLst" in str(te):
            print(f"FORMATTING ISSUE IN: {f}")
        error_messages.append(
            f"{rtype}:{rule_id}: Invalid xlsx file due to formatting/set filters: "
            f"<{pos_neg_folder}/{two_digit_folder}/data/{f.name}>"
        )
    except ValueError as ve:
        if "file format cannot be determined" in str(ve):
            print(f"FILE FORMAT ISSUE: {f}")
            error_messages.append(
                f"{rtype}:{rule_id}: Invalid xlsx file format: " f"<{pos_neg_folder}/{two_digit_folder}/data/{f.name}>"
            )
        else:
            raise


def results_folder_validation(
    error_messages: list,
    json_extension: str,
    subfolder_data_results_path: str,
    two_digit_folder: str,
    rtype: str,
    rule_id: str,
    pos_neg_folder: str,
):
    for file in os.scandir(subfolder_data_results_path):
        if file.is_file() and not (is_xlsx_file(file.name) or file.name.endswith(json_extension)):
            error_messages.append(
                f"{rtype}:{rule_id}: Non-xlsx and non-xml file found in: "
                f"<{pos_neg_folder}/{two_digit_folder}/results>"
            )
    for f in os.scandir(subfolder_data_results_path):
        if f.is_dir():
            error_messages.append(
                f"{rtype}:{rule_id}: Unexpected folder found: <{pos_neg_folder}/{two_digit_folder}/results/{f.name}>"
            )


def get_immediate_subfolders(folder_path):
    return sorted([f.name for f in os.scandir(folder_path) if f.is_dir()])


def is_xlsx_file(file_name: str) -> bool:
    return any(file_name.lower().endswith(ext) for ext in XLSX_FILE_EXTENSIONS)


def is_xml_file(file_name: str) -> bool:
    return file_name.lower().endswith(".xml")


def print_invalid(rtype: str, path: str):
    error_messages = []
    missing_lib_sheets = []
    missing_ds_sheets = []
    root_folder = f"{path}/{rtype}/"
    subfolders = get_immediate_subfolders(root_folder)
    for rulefolder in subfolders:
        validate_folder_structure(
            rtype,
            root_folder + rulefolder,
            rule_id=rulefolder,
            error_messages=error_messages,
            missing_lib_sheets=missing_lib_sheets,
            missing_ds_sheets=missing_ds_sheets,
        )

    # error_messages = [msg for msg in error_messages if "Unexpected file" in msg]
    # error_messages = [msg for msg in error_messages if "folder" in msg]
    print(f"Found {len(error_messages)} issues in {rtype} folder structure.")

    output_file = f"{path}/___{rtype}_sharepoint_issues.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for item in error_messages:
            f.write(f"{item}\n")

    missing_lib_file = f"{path}/___{rtype}_missing_lib.txt"
    with open(missing_lib_file, "w", encoding="utf-8") as f:
        for item in missing_lib_sheets:
            f.write(f"{item}\n")

    missing_ds_file = f"{path}/___{rtype}_missing_ds.txt"
    with open(missing_ds_file, "w", encoding="utf-8") as f:
        for item in missing_ds_sheets:
            f.write(f"{item}\n")


sp_path = os.path.expanduser("~") + "/data/CORE/CDISC_Sharepoint_dump_20250916"
# print_invalid("script_testing", sp_path)
print_invalid("SDTMIG", sp_path)
# print_invalid("ADAMIG", sp_path)
# print_invalid("FDA Business Rules", sp_path)
# print_invalid("FDA Validator Rules", sp_path)
