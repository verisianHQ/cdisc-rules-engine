import csv
import os
import re
import pandas as pd
from datetime import datetime
from dataclasses import dataclass

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.services import logger
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


B3_FIXED_WIDTH_COLSPECS = {
    B3WHODrugFiles.INA.value: [(0, 7), (7, 8), (8, 118)],
    B3WHODrugFiles.DDA.value: [(0, 6), (6, 8), (8, 11), (11, 12), (12, 19)],
    B3WHODrugFiles.DD.value: [
        (0, 6),
        (6, 8),
        (8, 11),
        (11, 12),
        (12, 13),
        (13, 15),
        (15, 19),
        (19, 24),
        (24, 26),
        (26, 27),
        (27, 30),
        (30, 1530),
    ],
}

ATC_CODE_LENGTH_TO_LEVEL = {1: "1", 3: "2", 4: "3", 5: "4", 7: "5"}

B3_DD_PREFIX = re.compile(r"^\d{12}.{18}")


class WhoDrugReader:
    """Reader for the WHO Drug dictionary."""

    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _resolve_file(self, file_name: str) -> tuple[str, str | None] | None:
        for extension in (".csv", ".txt"):
            file_path = f"{self.dictionary_path}/{file_name}{extension}"
            if os.path.exists(file_path):
                with open(file_path, mode="r", encoding="utf-8") as f:
                    first_line = f.readline()
                if "\t" in first_line:
                    return file_path, "\t"
                if extension == ".txt" and file_name in B3_FIXED_WIDTH_COLSPECS:
                    first_field_width = B3_FIXED_WIDTH_COLSPECS[file_name][0][1]
                    if "," not in first_line[: first_field_width + 1]:
                        return file_path, None
                return file_path, ","
        return None

    def _read_file(self, file_name: str, col_map: dict[int, str]) -> pd.DataFrame | None:
        resolved = self._resolve_file(file_name)
        if resolved is None:
            return None
        file_path, delimiter = resolved
        read_kwargs = {
            "header": None,
            "dtype": str,
            "usecols": list(col_map.keys()),
            "names": list(col_map.values()),
        }
        if delimiter is None:
            return pd.read_fwf(file_path, colspecs=B3_FIXED_WIDTH_COLSPECS[file_name], **read_kwargs)
        return pd.read_csv(file_path, sep=delimiter, **read_kwargs)

    def _count_fields(self, file_name: str) -> int | None:
        resolved = self._resolve_file(file_name)
        if resolved is None or resolved[1] is None:
            return None
        file_path, delimiter = resolved
        with open(file_path, mode="r", encoding="utf-8-sig") as f:
            return len(next(csv.reader([f.readline().rstrip("\r\n")], delimiter=delimiter)))

    def _extract_version_metadata(self) -> WhoDrugVersionMetadata:
        """Extract metadata from the Version file."""
        resolved = self._resolve_file(UniversalWhoDrugFiles.VERSION.value)
        if resolved is None:
            raise FileNotFoundError(f"WHODrug Version file not found in {self.dictionary_path}")
        file_path, _ = resolved
        with open(file_path, mode="r", encoding="utf-8-sig") as f:
            version_info = f.readline().strip().strip('"')

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

    def _apply_file_mapping(
        self,
        input_df: pd.DataFrame,
        file_name: str,
        col_map: dict[int, str],
        join_key: str,
        target_col: str,
    ) -> pd.DataFrame:

        df = self._read_file(file_name, col_map)

        if df is not None:
            df[join_key] = df[join_key].str.strip()

            return pd.merge(input_df, df, on=join_key, how="left")

        final_df = input_df.copy()
        final_df[target_col] = None
        return final_df

    def _merge_short_dd(self, input_df: pd.DataFrame) -> pd.DataFrame:
        file_path, delimiter = self._resolve_file(B3WHODrugFiles.DD.value)
        with open(file_path, mode="rb") as f:
            raw = f.read()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252")

        linked, unlinked = [], set()
        for line in text.splitlines():
            first_field = line.split(delimiter)[0]
            if B3_DD_PREFIX.match(first_field):
                linked.append((first_field[:6], first_field[30:].strip()))
            elif first_field.strip():
                unlinked.add(first_field.strip())

        dd_df = pd.DataFrame(linked, columns=["drug_rec_num", "drug_name"])
        final_df = pd.merge(input_df, dd_df, on="drug_rec_num", how="left")
        if unlinked:
            logger.warning(
                f"{len(unlinked)} WHODrug drug names in {file_path} have no drug record number and can't be "
                "linked to ATC codes; code-term pair and hierarchy checks won't match them."
            )
            final_df = pd.concat([final_df, pd.DataFrame({"drug_name": sorted(unlinked)})], ignore_index=True)
        return final_df

    def process_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        final_df = self._process_atc_data(metadata)
        return final_df

    def _process_atc_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        """
        Reads the active ingredient data for C3 and B3 formats.
        Processes the ATC hierarchy levels into flattened level 4 codes.
        Merges them to include the format's primary identifier and drug name.
        """
        atc_file_name = (
            C3WHODrugFiles.ATC.value if metadata.format == WhoDrugFormats.C3.value else B3WHODrugFiles.INA.value
        )
        if self._count_fields(atc_file_name) == 2:
            atc_col_map = {0: "atc_code", 1: "atc_text"}
        else:
            atc_col_map = {0: "atc_code", 1: "level", 2: "atc_text"}
        atc_df = self._read_file(atc_file_name, atc_col_map)
        if atc_df is None:
            raise FileNotFoundError(f"WHODrug file {atc_file_name} not found in {self.dictionary_path}")

        atc_df["atc_code"] = atc_df["atc_code"].str.strip()
        atc_df["atc_text"] = atc_df["atc_text"].str.strip()
        if "level" not in atc_df:
            atc_df["level"] = atc_df["atc_code"].str.len().map(ATC_CODE_LENGTH_TO_LEVEL)
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
            med_prod_df = self._apply_file_mapping(
                input_df=result_df,
                file_name=C3WHODrugFiles.ThG.value,
                col_map={1: "atc_code", 4: "med_prod_id"},
                join_key="atc_code",
                target_col="med_prod_id",
            )
            final_df = self._apply_file_mapping(
                input_df=med_prod_df,
                file_name=C3WHODrugFiles.MP.value,
                col_map={0: "med_prod_id", 8: "drug_name"},
                join_key="med_prod_id",
                target_col="drug_name",
            )

        elif metadata.format == WhoDrugFormats.B3.value:
            drug_rec_df = self._apply_file_mapping(
                input_df=result_df,
                file_name=B3WHODrugFiles.DDA.value,
                col_map={0: "drug_rec_num", 4: "atc_code"},
                join_key="atc_code",
                target_col="drug_rec_num",
            )
            dd_field_count = self._count_fields(B3WHODrugFiles.DD.value)
            if dd_field_count is not None and dd_field_count < len(B3_FIXED_WIDTH_COLSPECS[B3WHODrugFiles.DD.value]):
                final_df = self._merge_short_dd(drug_rec_df)
            else:
                final_df = self._apply_file_mapping(
                    input_df=drug_rec_df,
                    file_name=B3WHODrugFiles.DD.value,
                    col_map={0: "drug_rec_num", 11: "drug_name"},
                    join_key="drug_rec_num",
                    target_col="drug_name",
                )

        else:
            raise ValueError(f"Unsupported format: {metadata.format}")

        final_df = final_df.rename(columns={"atc_code": "term_code", "drug_name": "term_name"})
        return final_df
