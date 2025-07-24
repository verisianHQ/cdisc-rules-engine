from abc import ABC, abstractmethod
from csv import DictReader
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any


class BaseReader(ABC):
    """
    Abstract base class for all data readers.
    Provides common functionality for reading and parsing files.
    """

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self._validate_file()
        self.metadata = self._extract_metadata()

    def _validate_file(self) -> None:
        """Validate that the file exists and is accessible."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        if not self.file_path.is_file():
            raise ValueError(f"Path is not a file: {self.file_path}")

    @abstractmethod
    def _extract_metadata(self) -> Dict[str, Any]:
        """
        Extract metadata from the file name or content.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        """
        Read the file and return serialised data.
        Must be implemented by subclasses.
        """
        pass

    def _read_excel(self) -> List[Dict[str, Any]]:
        """
        Common excel file reading functionality.
        Used by subclasses to read csv/tsv/xlsx/xls files (e.g. metadata standards, terminology).
        """
        data = []
        with open(self.file_path, "r", encoding="utf-8") as file:
            if self.file_path.suffix in [".csv", ".tsv"]:
                reader = DictReader(file)
                for row in reader:
                    cleaned_row = {k: v.strip() if v else None for k, v in row.items()}
                    data.append(cleaned_row)
            elif self.file_path.suffix in [".xlsx", ".xls"]:
                df = pd.read_excel(self.file_path)
                data = df.to_dict(orient="records")
            else:
                raise ValueError(
                    f"Unsupported file type: {self.file_path.suffix}. Supported types are: .csv, .tsv, .xlsx, .xls"
                )
        return data

    def _read_sas(self) -> List[Dict[str, Any]]:
        """
        Common SAS file reading functionality.
        Used by subclasses to read xpt/sas7bdat files.
        """
        try:
            if self.file_path.suffix == ".xpt":
                df = pd.read_sas(self.file_path, format="xport", encoding="utf-8")
            elif self.file_path.suffix == ".sas7bdat":
                df = pd.read_sas(self.file_path, format="sas7bdat", encoding="utf-8")
            else:
                raise ValueError(
                    f"Unsupported SAS file type: {self.file_path.suffix}. " f"Supported types are: .xpt, .sas7bdat"
                )

            data = df.where(df.notna(), None).to_dict(orient="records")
            return data
        except Exception as e:
            raise ValueError(f"Error reading SAS file {self.file_path}: {str(e)}")
