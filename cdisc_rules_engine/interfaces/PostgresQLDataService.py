from typing import Optional, Union
import pandas as pd
import pandasql as ps

from pathlib import Path

from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.interfaces.SQLDataService import SQLDataService
from cdisc_rules_engine.models.TestDataset import TestDataset
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer


class PostgresQLDataService(SQLDataService):

    def __init__(
        self,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
        content_dfs: dict[pd.DataFrame] = None,
        metadata_df: pd.DataFrame = None,
        library_metadata: LibraryMetadataContainer = None,
    ):
        super().__init__(datasets_path, define_xml_path, terminology_paths)
        self.content_dfs = content_dfs
        self.metadata_df = metadata_df
        self.library_metadata = library_metadata if library_metadata else LibraryMetadataContainer()
        self.psql = ps.PandaSQL()

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
        library_metadata: LibraryMetadataContainer = None,
    ) -> None:
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables, setting path to "memory"
        """
        content_dfs = {}
        metadata_df = pd.DataFrame()
        for test_dataset in test_datasets:
            # Collect content
            cdf = pd.DataFrame.from_records(test_dataset["records"])
            cdf.columns = [col.lower() for col in cdf.columns]
            content_dfs[test_dataset["filename"]] = cdf

            # Collect variable metadata
            for test_variable in test_dataset["variables"]:
                new_row = pd.DataFrame(
                    {
                        "dataset_id": [test_dataset["filename"]],
                        "filepath": [test_dataset["filepath"]],
                        "domain": [test_dataset["filename"].split(".")[0].upper()],
                        "name": [test_variable["name"]],
                        "label": [test_variable["label"]],
                        "type": [test_variable["type"]],
                        "length": [test_variable["length"]],
                    }
                )
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)

        return cls(datasets_path, define_xml_path, terminology_paths, content_dfs, metadata_df, library_metadata)

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

    def is_supplemental_dataset(self, dataset_id: str) -> bool:
        """
        Check if the dataset is a supplemental dataset.
        """
        metadata_df = self.metadata_df
        query = f"""
            SELECT domain
            FROM metadata_df
            WHERE dataset_id = '{dataset_id}'
            LIMIT 1
        """
        result_df = self._safe_psql(query, {"metadata_df": metadata_df})
        if result_df.empty:
            return False
        domain = result_df["domain"].iat[0]
        return domain.startswith(SUPPLEMENTARY_DOMAINS)

    # TODO: implement once we get an answer from Sam/Gerry
    def is_split_dataset(self, dataset_id: str) -> bool:
        return False

    def get_unsplit_name(self, dataset_id: str) -> str:
        return self.get_domain(dataset_id)

    def get_domain(self, dataset_id: str) -> Union[str, None]:
        """
        Return dataset domain based on dataset_id.
        """
        return self._get_val_from_var_from_metadata(dataset_id, "domain")

    def get_full_path(self, dataset_id: str) -> Union[str, None]:
        """
        Return dataset full_path based on dataset_id.
        """
        return self._get_val_from_var_from_metadata(dataset_id, "full_path")

    def get_rdomain(self, dataset_id: str) -> Union[str, None]:
        """
        Return dataset rdomain based on dataset_id.
        """
        return self._get_val_from_var_from_data(dataset_id, "rdomain")

    def get_filename(self, dataset_id: str) -> Union[str, None]:
        """
        Return dataset filename based on dataset_id.
        """
        return self._get_val_from_var_from_data(dataset_id, "filename")

    def _safe_psql(self, query: str, env: dict) -> pd.DataFrame:
        try:
            return self.psql(query, env)
        except ps.PandaSQLException:
            return pd.DataFrame()

    def _get_val_from_var_from_metadata(self, dataset_id: str, col: str) -> Union[str, None]:
        metadata_df = self.metadata_df
        query = f"""
            SELECT {col}
            FROM metadata_df
            WHERE dataset_id = '{dataset_id}'
            LIMIT 1
        """
        result_df = self._safe_psql(query, {"metadata_df": metadata_df})
        if result_df.empty:
            return None
        ret = result_df[col].iat[0]
        return ret

    def _get_val_from_var_from_data(self, dataset_id: str, col: str) -> Union[str, None]:
        dataset = self.content_dfs.get(dataset_id, None)
        if dataset is None:
            return None
        query = f"""
            SELECT {col}
            FROM dataset
            LIMIT 1
        """
        result_df = self._safe_psql(query, {"dataset": dataset})
        if result_df.empty:
            return None
        ret = result_df[col].iat[0]
        return ret

    # TODO: once we have a standards data model for the standard metadata,
    # this should take the standards version and dataset_id as arguments and try the class conversion
    def get_dataset_class(
        self,
        dataset_id: str,
        # dataset: DatasetInterface,
        # file_path: str,
        # datasets: Iterable[SDTMDatasetMetadata],
        # dataset_metadata: SDTMDatasetMetadata,
    ) -> Optional[str]:
        # if self.library_metadata.standard_metadata:
        #     class_data, _ = get_class_and_domain_metadata(
        #         self.library_metadata.standard_metadata,
        #         dataset_metadata.unsplit_name,
        #     )
        #     name = class_data.get("name")
        #     if name:
        #         return convert_library_class_name_to_ct_class(name)
        # return self._handle_special_cases(dataset, dataset_metadata, file_path, datasets)
        return "FINDINGS"

    def _handle_special_cases(
        self,
        # dataset: DatasetInterface,
        # dataset_metadata: SDTMDatasetMetadata,
        # file_path: str,
        # datasets: Iterable[SDTMDatasetMetadata],
    ):
        # if self._contains_topic_variable(dataset, dataset_metadata.domain, "TERM"):
        #     return EVENTS
        # if self._contains_topic_variable(dataset, dataset_metadata.domain, "TRT"):
        #     return INTERVENTIONS
        # if self._contains_topic_variable(dataset, dataset_metadata.domain, "QNAM"):
        #     return RELATIONSHIP
        # if self._contains_topic_variable(dataset, dataset_metadata.domain, "TESTCD"):
        #     if self._contains_topic_variable(dataset, dataset_metadata.domain, "OBJ"):
        #         return FINDINGS_ABOUT
        #     return FINDINGS
        # if self._is_associated_persons(dataset):
        #     return self._get_associated_persons_inherit_class(file_path, datasets, dataset_metadata.domain)
        # return None
        return ""
