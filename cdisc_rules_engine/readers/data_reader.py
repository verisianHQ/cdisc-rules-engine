from pathlib import Path
from typing import List, Dict, Any, Optional
from cdisc_rules_engine.readers.base_reader import BaseReader
from dataclasses import dataclass
import pandas as pd


@dataclass
class ClinicalDataMetadata:
    """Metadata extracted from clinical data filenames and content."""

    domain: str
    standard_type: str
    file_format: str
    study_id: Optional[str] = None
    dataset_label: Optional[str] = None


class DataReader(BaseReader):
    """
    Reader for clinical data files (XPT and SAS7BDAT).
    Handles both ADaM and SDTM datasets.
    """

    ADAM_DOMAINS = ["adae", "adef", "adsl", "adtte"]
    SDTM_DOMAINS = ["ae", "dm", "ex", "lb", "suppdm", "ta", "td", "te", "ti", "ts", "tv", "xp"]

    SUPPORTED_EXTENSIONS = [".xpt", ".sas7bdat"]

    def __init__(self, file_path: str, variable_metadata_path: Optional[str] = None):
        self.variable_metadata_path = variable_metadata_path
        self.variable_metadata = None
        super().__init__(file_path)

        if self.variable_metadata_path:
            self._load_variable_metadata()

    def _extract_metadata(self) -> ClinicalDataMetadata:
        """Extract metadata from filename and file content."""
        if self.file_path.suffix not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file format: {self.file_path.suffix}. "
                f"Supported formats: {', '.join(self.SUPPORTED_EXTENSIONS)}"
            )

        domain = self.file_path.stem.lower()

        standard_type = "ADaM" if domain in self.ADAM_DOMAINS else "SDTM"

        file_format = self.file_path.suffix[1:]

        metadata = ClinicalDataMetadata(domain=domain.upper(), standard_type=standard_type, file_format=file_format)
        self._extract_file_metadata(metadata)

        return metadata

    def _extract_file_metadata(self, metadata: ClinicalDataMetadata) -> None:
        """Extract additional metadata from file content."""
        try:
            if self.file_path.suffix == ".xpt":
                df = pd.read_sas(self.file_path, format="xport", encoding="utf-8", chunksize=1)
                first_chunk = next(df)
            else:
                df = pd.read_sas(self.file_path, format="sas7bdat", encoding="utf-8", chunksize=1)
                first_chunk = next(df)
            if "STUDYID" in first_chunk.columns and not first_chunk["STUDYID"].empty:
                metadata.study_id = str(first_chunk["STUDYID"].iloc[0])
        except Exception:
            pass

    def _load_variable_metadata(self) -> None:
        """Load variable metadata from Excel file."""
        try:
            metadata_path = Path(self.variable_metadata_path)

            if self.metadata.standard_type == "ADaM":
                file_name = "ADAM_METADATA_MODIFIED.xlsx"
            else:
                file_name = "SDTM_METADATA_MODIFIED.xlsx"

            if metadata_path.is_dir():
                metadata_file = metadata_path / file_name
            else:
                metadata_file = metadata_path

            if metadata_file.exists():
                df = pd.read_excel(metadata_file, sheet_name="VARIABLE_METADATA")
                domain_df = df[df.iloc[:, 0] == self.metadata.domain]
                self.variable_metadata = domain_df.iloc[:, 2].tolist()
        except Exception as e:
            print(f"Warning: Could not load variable metadata: {e}")

    def read(self) -> Dict[str, Any]:
        """Read the clinical data file and return structured data."""
        raw_data = self._read_sas()
        variables = self._extract_variables(raw_data)
        result = {
            "metadata": {
                "name": self.file_path.name,
                "domain": self.metadata.domain,
                "standard_type": self.metadata.standard_type,
                "file_format": self.metadata.file_format,
                "study_id": self.metadata.study_id,
                "dataset_label": self.metadata.dataset_label,
                "record_count": len(raw_data),
                "variable_count": len(variables),
            },
            "data": raw_data,
            "variables": variables,
        }

        if self.variable_metadata:
            result["expected_variables"] = self.variable_metadata
            result["missing_variables"] = [
                var for var in self.variable_metadata if var not in [v["name"] for v in variables]
            ]
            result["unexpected_variables"] = [v["name"] for v in variables if v["name"] not in self.variable_metadata]

        return result

    def _extract_variables(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract variable information from the data."""
        if not data:
            return []

        first_record = data[0]
        variables = []

        for i, (name, _) in enumerate(first_record.items()):
            var_info = {"name": name, "order": i + 1, "type": self._infer_type(data, name)}

            var_info["label"] = None
            variables.append(var_info)

        return variables

    def _infer_type(self, data: List[Dict[str, Any]], column: str) -> str:
        """Infer variable type from sample values."""
        sample_size = min(10, len(data))
        values = [row.get(column) for row in data[:sample_size] if row.get(column) is not None]

        if not values:
            return "Unknown"

        try:
            all(float(v) for v in values if v is not None)
            return "Num"
        except (ValueError, TypeError):
            return "Char"

    def validate_against_metadata(self) -> Dict[str, Any]:
        """Validate the dataset against variable metadata if available."""
        if not self.variable_metadata:
            return {"status": "No metadata available for validation"}

        result = self.read()

        validation = {
            "domain": self.metadata.domain,
            "expected_variable_count": len(self.variable_metadata),
            "actual_variable_count": len(result["variables"]),
            "missing_variables": result.get("missing_variables", []),
            "unexpected_variables": result.get("unexpected_variables", []),
            "is_valid": len(result.get("missing_variables", [])) == 0,
        }

        return validation
