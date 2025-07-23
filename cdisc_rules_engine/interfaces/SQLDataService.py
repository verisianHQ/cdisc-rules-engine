from abc import ABC, abstractmethod
from pathlib import Path

from cdisc_rules_engine.models.TestDataset import TestDataset


class SQLDataService(ABC):

    def __init__(
        self,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
    ):
        """
        Initialize the data service.

        Parameters:
        - datasets_path: Path to the folder containing datasets.
        - define_xml_path: Path to the Define-XML file.
        - terminology_paths: A dictionary with keys:
            'whodrug', 'loinc', 'medrt', 'meddra', 'unii',
            each mapped to a Path representing the respective folder.
        """
        self.datasets_path = datasets_path
        self.define_xml_path = define_xml_path
        self.terminology_paths = terminology_paths

        self._create_sql_tables_from_dataset_paths()
        self._create_definexml_tables()
        self._create_terminology_tables()
        self._create_standards_tables()
        self._create_codelist_tables()

    @abstractmethod
    def from_list_of_testdatasets(cls, test_datasets: list[TestDataset]) -> None:
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables, setting path to "memory"
        """
        pass

    @abstractmethod
    def from_list_of_dicts(
        cls, data_dicts: dict[str, list[dict]], define_xml_path: Path, terminology_paths: dict
    ) -> None:
        """
        Constructor for tests, passing in dict
        (key = dataset name, value = list of row dicts)
        and create corresponding SQL tables, setting path to "memory"
        """
        pass

    @abstractmethod
    def from_list_of_records(
        cls, records: dict[str, dict[str, list[any]]], define_xml_path: Path, terminology_paths: dict
    ) -> None:
        """
        Constructor for tests, passing in dict
        (key = dataset name, value = dict (key = column name, value = list of values))
        and create corresponding SQL tables, setting path to "memory"
        """
        pass

    @abstractmethod
    def _create_sql_tables_from_dataset_paths(self) -> None:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        pass

    @abstractmethod
    def _create_definexml_tables(self) -> None:
        """
        Read the self.define_xml_path and create corresponding SQL tables.
        """
        pass

    @abstractmethod
    def _create_terminology_tables(self) -> None:
        """
        Iterate through self.terminology_paths dict
        and create corresponding SQL tables if paths exist.
        """
        pass

    @abstractmethod
    def _create_standards_tables(self) -> None:
        """
        Create all necessary SQL tables for IG standards.
        """
        pass

    @abstractmethod
    def _create_codelist_tables(self) -> None:
        """
        Create all necessary SQL tables for CDISC codelists.
        """
        pass
