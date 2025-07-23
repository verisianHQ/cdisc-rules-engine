from pathlib import Path

from cdisc_rules_engine.interfaces import SQLDataService


class PostgresQLDataService(SQLDataService):

    def from_list_of_dict(
        cls, data_dicts: dict[str, list[dict]], define_xml_path: Path, terminology_paths: dict
    ) -> None:
        """
        Constructor for tests, passing in dict
        (key = dataset name, value = list of row dicts)
        and create corresponding SQL tables, setting path to "memory"
        """
        pass

    def from_list_of_records(
        cls, records: dict[str, dict[str, list[any]]], define_xml_path: Path, terminology_paths: dict
    ) -> None:
        """
        Constructor for tests, passing in dict
        (key = dataset name, value = dict (key = column name, value = list of values))
        and create corresponding SQL tables, setting path to "memory"
        """
        pass

    def _create_sql_tables_from_dataset_paths(self) -> None:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        pass

    def _create_definexml_tables(self) -> None:
        """
        Read the self.define_xml_path and create corresponding SQL tables.
        """
        pass

    def _create_terminology_tables(self) -> None:
        """
        Iterate through self.terminology_paths dict
        and create corresponding SQL tables if paths exist.
        """
        pass

    def _create_standards_tables(self) -> None:
        """
        Create all necessary SQL tables for IG standards.
        """
        pass

    def _create_codelist_tables(self) -> None:
        """
        Create all necessary SQL tables for CDISC codelists.
        """
        pass
