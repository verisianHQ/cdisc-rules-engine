import pandas as pd
import pandasql as ps

from pathlib import Path

from cdisc_rules_engine.interfaces.SQLDataService import SQLDataService
from cdisc_rules_engine.models.TestDataset import TestDataset


class PostgresQLDataService(SQLDataService):

    def __init__(
        self,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
        data_dfs: dict[str, pd.DataFrame] = None,
        metadata_df: pd.DataFrame = None,
    ):
        super().__init__(datasets_path, define_xml_path, terminology_paths)
        self.data_dfs = data_dfs
        self.metadata_df = metadata_df
        self.psql = ps.PandaSQL()

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
        data_dfs = {}
        metadata_df = pd.DataFrame()
        for test_dataset in test_datasets:
            # Collect content
            ddf = pd.DataFrame.from_records(test_dataset["records"])
            ddf.columns = [col.lower() for col in ddf.columns]
            data_dfs[test_dataset["filename"]] = ddf

            # Collect variable metadata
            for test_variable in test_dataset["variables"]:
                new_row = pd.DataFrame(
                    {
                        "filename": [test_dataset["filename"]],
                        "filepath": [test_dataset["filepath"]],
                        "dataset_id": [test_dataset["name"]],
                        "dataset_name": [test_dataset["name"]],
                        "dataset_label": [test_dataset["label"]],
                        "domain": [test_dataset["filename"].split(".")[0].upper()],
                        "name": [test_variable["name"]],
                        "label": [test_variable["label"]],
                        "type": [test_variable["type"]],
                        "length": [test_variable["length"]],
                    }
                )
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)

        return cls(datasets_path, define_xml_path, terminology_paths, data_dfs, metadata_df)

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
