from typing import Union

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.startup.populate_standards import (
    IG_DATASETS_TABLE_NAME,
)
from cdisc_rules_engine.utilities.ig_specification import IGSpecification


class DatasetMetadata:
    dataset_name: str
    dataset_label: str
    unsplit_name: str
    domain: str
    is_supp: bool
    is_relrec: bool
    is_co: bool
    is_split: bool
    related_domain: str  # Used for supp datasets


class DatasetMetadataManager:
    """
    Stores metadata about the input datasets being analyzed by the rules engine.
    """

    def __init__(self, standard: IGSpecification, pgi: PostgresQLInterface):
        self._dataset_metadata: dict[str, DatasetMetadata] = {}
        self.standard = standard
        self.pgi = pgi
        self._domains = None

    def _load_domains(self):
        """
        Load domains from the PostgresQLInterface.
        """
        if self._domains is None:
            ig_domain_table = self.pgi.schema.get_table(IG_DATASETS_TABLE_NAME)
            self.pgi.execute_sql(
                f"""SELECT
                        {ig_domain_table.get_column_hash("datasset_name")} AS domain,
                        {ig_domain_table.get_column_hash("class")} AS class,
                        {ig_domain_table.get_column_hash("dataset_label")} AS label
                    FROM {ig_domain_table}
                    WHERE standard_type = '{self.standard.standard_version}'"""
            )
            results = self.pgi.fetchall()
            self._domains = {row["domain"].lower(): row for row in results}

    def add_dataset(self, dataset_name: str):
        """
        Generates and adds metadata for a dataset
        """
        self._load_domains()
        self._dataset_metadata[dataset_name.lower()] = DatasetMetadataManager.build_metadata(
            dataset_name, self._domains
        )

    def get_dataset_metadata(self, dataset_name: str) -> Union[DatasetMetadata, None]:
        """
        Retrieve metadata for a dataset.
        """
        return self._dataset_metadata.get(dataset_name.lower(), None)

    @staticmethod
    def build_metadata(name: str, domain_lookup: dict) -> DatasetMetadata:
        """
        Construct the metadata for the dataset.
        """
        original = name

        name = name.lower()
        if name.startswith("supp"):
            is_supp = True
            name = name[4:]
            domain = "suppqual"

        if name.startswith("fa"):
            is_findings_about = True
            name = name[2:]
            domain = "fa"

        if name.startswith("relrec"):
            is_relrec = True
            domain = "relrec"
        elif name.startswith("co"):
            is_comments = True
            domain = "co"
        elif name.startswith("relspec"):

        else:
            if name[:2] in domain_lookup:
                domain = name[:2]
