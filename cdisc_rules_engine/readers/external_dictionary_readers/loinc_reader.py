import os
import re
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface

_VERSION_RE = re.compile(r"(\d+(?:\.\d+)+)")


@dataclass
class LoincVersionMetadata:
    version: Optional[str]


class LoincReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> LoincVersionMetadata:
        """Extract metadata from the LOINC directory."""
        base_dir = os.path.basename(os.path.normpath(self.dictionary_path))
        match = _VERSION_RE.search(base_dir)
        if not match:
            for file in os.listdir(self.dictionary_path):
                if file.startswith("Loinc_") and file.endswith("_DifferenceReport.pdf"):
                    match = _VERSION_RE.search(file)
                    break
        return LoincVersionMetadata(version=match.group(1) if match else None)

    def process_data(self, metadata: LoincVersionMetadata = None) -> pd.DataFrame:
        """
        Reads the Loinc.csv file and returns a dataframe with the mapped term code, term name, version, status.
        """
        file_path = f"{self.dictionary_path}/LoincTable/Loinc.csv"
        if not os.path.exists(file_path):
            file_path = f"{self.dictionary_path}/Loinc.csv"

        df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

        df.rename(
            columns={
                "LOINC_NUM": "term_code",
                "COMPONENT": "term_name",
                "VersionLastChanged": "version",
                "STATUS": "status",
            },
            inplace=True,
        )

        return df
