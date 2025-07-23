import pandas as pd

from pathlib import Path

from cdisc_rules_engine.interfaces.SQLDataService import SQLDataService
from cdisc_rules_engine.models.TestDataset import TestDataset


class PostgresQLDataService(SQLDataService):

    def __init__(
        self,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
        content_dfs: list[pd.DataFrame] = None,
        metadata_dfs: list[pd.DataFrame] = None,
    ):
        super().__init__(datasets_path, define_xml_path, terminology_paths)
        self.content_dfs = content_dfs
        self.metadata_dfs = metadata_dfs

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
    ) -> None:
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables, setting path to "memory"
        """
        content_dfs = []
        metadata_dfs = []
        for test_dataset in test_datasets:
            # Collect content
            cdf = pd.DataFrame.from_records(test_dataset["records"])
            cdf.columns = [col.lower() for col in cdf.columns]
            content_dfs.append(cdf)

            # Collect variable metadata
            mdf = pd.DataFrame()
            for test_variable in test_dataset["variables"]:
                new_row = pd.DataFrame(
                    {
                        "domain": [test_dataset["filename"].split(".")[0].upper()],
                        "name": [test_variable["name"]],
                        "label": [test_variable["label"]],
                        "type": [test_variable["type"]],
                        "length": [test_variable["length"]],
                    }
                )
                mdf = pd.concat([mdf, new_row], ignore_index=True)
            metadata_dfs.append(mdf)

        return cls(datasets_path, define_xml_path, terminology_paths, content_dfs, metadata_dfs)

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
