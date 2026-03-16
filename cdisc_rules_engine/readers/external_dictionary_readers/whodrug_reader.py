import os
import csv
import re
import pandas as pd
from datetime import datetime
from dataclasses import dataclass

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.whodrug_files import (
    WhoDrugFormats,
    UniversalWhoDrugFiles,
    B3WHODrugFiles,
    C3WHODrugFiles,
)


@dataclass
class WhoDrugVersionMetadata:
    """Metadata extracted from codelist filenames."""

    product_type: str
    format: str
    version_date: str


class WhoDrugReader:
    """Reader for the WHO Drug dictionary."""

    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> WhoDrugVersionMetadata:
        """Extract metadata from the Version file."""
        file_path = f"{self.dictionary_path}/{UniversalWhoDrugFiles.VERSION.value}.csv"
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            version_info = next(reader)[0]

        version_pattern = (
            r"WHODRUG\s+(?P<product_type>\w+)\s+(?P<format>C3|B3)\s+(?P<version_date>\w+\s\d{1,2},\s\d{4})"
        )
        match = re.match(version_pattern, version_info)
        if match:
            product_type = match.group("product_type")
            format = match.group("format")
            version_date = datetime.strptime(match.group("version_date"), "%B %d, %Y").strftime("%Y%m%d")
            return WhoDrugVersionMetadata(product_type=product_type, format=format, version_date=version_date)
        else:
            raise ValueError(f"Version information in {file_path} does not match expected format.")

    def process_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        """
        Reads the active ingredient data for C3 and B3 formats.
        Processes the ATC hierarchy levels into flattened level 4 codes.
        Merges them to include the format's primary identifier.
        """
        atc_file_name = (
            C3WHODrugFiles.ATC.value if metadata.format == WhoDrugFormats.C3.value else B3WHODrugFiles.INA.value
        )
        atc_file_path = f"{self.dictionary_path}/{atc_file_name}.csv"
        atc_df = pd.read_csv(atc_file_path, header=None, dtype=str, names=["atc_code", "level", "atc_text"])
        atc_df["atc_code"] = atc_df["atc_code"].str.strip()
        atc_df["atc_text"] = atc_df["atc_text"].str.strip()
        code_to_text_map = dict(zip(atc_df["atc_code"], atc_df["atc_text"]))
        level_4_codes = atc_df[atc_df["level"] == "4"]
        result_df = pd.DataFrame({"atc_code": level_4_codes["atc_code"]})
        level_to_length = {
            "level_1": 1,
            "level_2": 3,
            "level_3": 4,
            "level_4": 5,
        }

        for level, length in level_to_length.items():
            result_df[level] = result_df["atc_code"].apply(
                lambda x: code_to_text_map.get(x[:length], None) if len(x) >= length else None
            )

        if metadata.format == WhoDrugFormats.C3.value:
            final_df = self._get_med_prod_id_mapping(result_df)
        elif metadata.format == WhoDrugFormats.B3.value:
            final_df = self._get_drug_rec_num_mapping(result_df)

        return final_df

    def _get_med_prod_id_mapping(self, atc_df: pd.DataFrame) -> pd.DataFrame:
        thg_file_path = f"{self.dictionary_path}/{C3WHODrugFiles.ThG.value}.csv"
        if os.path.exists(thg_file_path):
            thg_df = pd.read_csv(
                thg_file_path,
                header=None,
                dtype=str,
                names=["thg_id", "atc_code", "create_date", "official_atc_code", "med_prod_id"],
            )

            thg_subset = thg_df[["atc_code", "med_prod_id"]].copy()
            thg_subset["atc_code"] = thg_subset["atc_code"].str.strip()

            final_df = pd.merge(atc_df, thg_subset, on="atc_code", how="left")
        else:
            final_df = atc_df.copy()
            final_df["med_prod_id"] = None

        return final_df

    def _get_drug_rec_num_mapping(self, atc_df: pd.DataFrame) -> pd.DataFrame:
        dda_file_path = f"{self.dictionary_path}/{B3WHODrugFiles.DDA.value}.csv"
        if os.path.exists(dda_file_path):
            dda_df = pd.read_csv(
                dda_file_path,
                header=None,
                dtype=str,
                names=[
                    "drug_rec_num",
                    "seq_num_1",
                    "seq_num_2",
                    "check_digit",
                    "atc_code",
                    "year_quarter",
                    "official_atc_code",
                ],
            )
            dda_subset = dda_df[["drug_rec_num", "atc_code"]].copy()
            dda_subset["atc_code"] = dda_subset["atc_code"].str.strip()

            final_df = pd.merge(atc_df, dda_subset, on="atc_code", how="left")
        else:
            final_df = atc_df.copy()
            final_df["drug_rec_num"] = None

        return final_df
