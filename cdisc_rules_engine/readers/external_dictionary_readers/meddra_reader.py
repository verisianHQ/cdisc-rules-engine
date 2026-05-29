import os
import pandas as pd
from dataclasses import dataclass
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class MeddraVersionMetadata:
    version: str
    language: str
    date: str


class MeddraReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> MeddraVersionMetadata:
        """Extract metadata from the MedDRA readme file."""
        for file in os.listdir(self.dictionary_path):
            if file.startswith("!!readme_") and file.endswith(".txt"):
                # file is "!!readme_29_0_English.txt" we want version = "29.0" and language = "English"
                version, language = file.split("_", 1)[1].rsplit(".", 1)[0].rsplit("_", 1)
                version = version.replace("_", ".")
                with open(os.path.join(self.dictionary_path, file), "r", encoding="ISO-8859-1") as f:
                    lines = f.readlines()
                    date_str = lines[-2].strip()
                    date_obj = pd.to_datetime(date_str, format="%B %Y")
                    date = date_obj.strftime("%d-%m-%Y")
                return MeddraVersionMetadata(version=version, language=language, date=date)
        return MeddraVersionMetadata(version="unknown", language="unknown", date="unknown")

    def process_data(self, metadata: MeddraVersionMetadata) -> pd.DataFrame:
        """
        Reads the 5 term .asc files and concatenates them into a unified dataframe.
        """
        term_files = {"SOC": "soc.asc", "HLGT": "hlgt.asc", "HLT": "hlt.asc", "PT": "pt.asc", "LLT": "llt.asc"}

        dfs = []
        for term_type, file_name in term_files.items():
            path = None
            for root, dirs, files in os.walk(self.dictionary_path):
                if file_name in files:
                    path = os.path.join(root, file_name)
                    break
            if os.path.exists(path):
                df = pd.read_csv(path, sep="$", header=None, usecols=[0, 1], dtype=str, encoding="utf-8")
                df.columns = ["term_code", "term_name"]
                df["term_type"] = term_type
                df["version"] = ""
                df.at[0, "version"] = metadata.version
                df["language"] = ""
                df.at[0, "language"] = metadata.language
                df["date"] = ""
                df.at[0, "date"] = metadata.date
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True)
