import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.startup.populate_codelists import (
    populate_codelists,
)
from cdisc_rules_engine.data_service.startup.populate_standards import (
    populate_standards,
)
from cdisc_rules_engine.data_service.startup.populate_terminology import (
    populate_terminology,
)
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.utilities.ig_specification import IGSpecification
from cdisc_rules_engine.utilities.sql_dataset_preprocessor import SQLDatasetPreprocessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schemas"


@dataclass
class SQLDatasetMetadata:
    filename: str
    filepath: str
    dataset_id: str
    table_hash: str
    dataset_name: str
    dataset_label: str
    unsplit_name: str
    domain: str
    is_supp: bool
    rdomain: str
    variables: list[str]


class PostgresQLDataService:

    def __init__(
        self,
        postgres_interface: PostgresQLInterface,
        ig_specs: IGSpecification,
    ):
        super().__init__(ig_specs)
        self.pgi = postgres_interface

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        ig_specs: IGSpecification,
        datasets_path: Path = None,
        define_xml_path: str = "",
        terminology_paths: dict = None,
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        metadata_rows: list[dict[str, Union[str, int, float]]] = []

        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        # create metadata table in postgres
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))

        PostgresQLDataService._preprocess_data(pgi)
        instance = cls(
            pgi,
            ig_specs,
            datasets_path,
            define_xml_path,
            None,
            None,
            terminology_paths,
        )

        # generate timestamp
        timestamp = datetime.now().astimezone()
        for test_dataset in test_datasets:
            # Create schema and table:
            row_dicts = [
                dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())
            ]
            # force lower_case throughout
            table_name = test_dataset["name"].lower()
            row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

            schema = SqlTableSchema.from_metadata(test_dataset)
            pgi.create_table(schema)
            pgi.insert_data(table_name=table_name, data=row_dicts)

            # Collect variable metadata
            for test_variable in test_dataset["variables"]:
                name = test_dataset["name"]
                domain = row_dicts[0].get("domain") if row_dicts else None
                is_supp = test_dataset["name"].startswith(SUPPLEMENTARY_DOMAINS)
                rdomain = row_dicts[0].get("rdomain") if is_supp and row_dicts else None
                unsplit_name = PostgresQLDataService._get_unsplit_name(name, domain, rdomain)
                is_split = name != unsplit_name
                metadata_rows.append(
                    {
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "dataset_filename": test_dataset["filename"],
                        "dataset_filepath": test_dataset["filepath"],
                        "dataset_id": name.lower(),
                        "table_hash": name.lower(),
                        "dataset_name": name,
                        "dataset_label": test_dataset["label"],
                        "dataset_domain": domain,
                        "dataset_is_supp": is_supp,
                        "dataset_rdomain": rdomain,
                        "dataset_is_split": is_split,
                        "dataset_unsplit_name": unsplit_name,
                        "dataset_preprocessed": None,
                        "var_name": test_variable["name"].lower(),
                        "var_label": test_variable["label"],
                        "var_type": test_variable["type"],
                        "var_length": test_variable["length"],
                        "var_format": test_variable["format"],
                    }
                )

        # write metadata rows into DB
        pgi.insert_data(table_name="data_metadata", data=metadata_rows)

        return instance

    @staticmethod
    def add_test_dataset(
        pgi: PostgresQLInterface, table_name: str, column_data: dict[str, list[Union[str, int, float]]]
    ):
        # Check all the columns are the same length
        lengths = {len(v) for v in column_data.values()}
        if len(set(lengths)) != 1:
            raise ValueError("All input data columns must have the same length")

        # Create schema and table:
        schema_row = {
            col.lower(): next((val for val in values if val is not None), "") for col, values in column_data.items()
        }
        row_dicts = [dict(zip(column_data, values)) for values in zip(*column_data.values())]
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        schema = SqlTableSchema.from_data(table_name, schema_row)
        pgi.create_table(schema)

        pgi.insert_data(table_name=table_name, data=row_dicts)
        return schema

    @classmethod
    def instance(cls) -> "PostgresQLDataService":
        """
        Create a PostgresQLDataService instance with an initialized database.
        """
        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        instance = cls(postgres_interface=pgi, ig_specs=None)
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))
        populate_terminology(pgi)
        populate_codelists(pgi)
        populate_standards(pgi)
        return instance

    @classmethod
    def from_dataset_paths(
        cls,
        datasets_path: Path,
        ig_specs: IGSpecification,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
    ) -> "PostgresQLDataService":
        """
        Load test datasets from dataset_paths to be used during test execution
        """
        pgi = PostgresQLInterface()
        pgi.init_database()

        instance = cls(
            postgres_interface=pgi,
            ig_specs=ig_specs,
            datasets_path=datasets_path,
            define_xml_path=define_xml_path,
            terminology_paths=terminology_paths,
        )

        SQLDatasetPreprocessor.run(pgi)

        return instance

    def _build_metadata_rows(
        self, file_path: Path, table_name: str, metadata_info: dict, first_chunk: dict, timestamp: datetime
    ) -> list[dict]:
        """Build metadata rows for all variables in the dataset."""

        domain = first_chunk.get("domain", None)
        is_supp = domain.startswith(SUPPLEMENTARY_DOMAINS) if domain is not None else False
        rdomain = first_chunk.get("rdomain", None)
        unsplit_name = PostgresQLDataService._get_unsplit_name(table_name, domain, rdomain)
        is_split = table_name != unsplit_name

        metadata_rows = []
        for var_info in metadata_info["variables"]:
            metadata_rows.append(
                {
                    "created_at": timestamp,
                    "updated_at": timestamp,
                    "dataset_filename": file_path.name,
                    "dataset_filepath": str(file_path),
                    "dataset_id": table_name,
                    "dataset_name": table_name,
                    "dataset_label": metadata_info["metadata"].get("dataset_label", ""),
                    "dataset_domain": domain,
                    "dataset_is_supp": is_supp,
                    "dataset_rdomain": rdomain,
                    "dataset_is_split": is_split,
                    "dataset_unsplit_name": unsplit_name,
                    "dataset_preprocessed": None,
                    "var_name": var_info.get("name", "").lower(),
                    "var_label": var_info.get("label"),
                    "var_type": var_info.get("type") or var_info.get("ctype"),
                    "var_length": var_info.get("length"),
                    "var_format": var_info.get("format"),
                }
            )

        return metadata_rows

    def get_uploaded_dataset_ids(self) -> list[str]:
        query = "SELECT dataset_id FROM data_metadata GROUP BY dataset_id ORDER BY MIN(id);"
        self.pgi.execute_sql(query=query)
        results = self.pgi.fetch_all()
        return [res["dataset_id"] for res in results]

    def get_dataset_metadata(self, dataset_id: str) -> SQLDatasetMetadata:
        query = f"""
            SELECT *
            FROM data_metadata
            WHERE dataset_id = '{dataset_id}';
        """
        self.pgi.execute_sql(query=query)
        results = self.pgi.fetch_all()
        if not results:
            return None
        return SQLDatasetMetadata(
            filename=results[0].get("dataset_filename"),
            filepath=results[0].get("dataset_filepath"),
            dataset_id=results[0].get("dataset_id"),
            table_hash=results[0].get("table_hash"),
            dataset_name=results[0].get("dataset_name"),
            dataset_label=results[0].get("dataset_label"),
            unsplit_name=results[0].get("dataset_unsplit_name"),
            domain=results[0].get("dataset_domain"),
            is_supp=results[0].get("dataset_is_supp"),
            rdomain=results[0].get("dataset_rdomain"),
            variables=[res["var_name"] for res in results],
        )

    def get_dataset_for_rule(self, dataset_metadata: SQLDatasetMetadata, rule: dict) -> str:
        """Get or create preprocessed dataset based on rule requirements."""
        datasets = rule.get("datasets", [])
        if not datasets:
            return dataset_metadata.dataset_id

        left_id = dataset_metadata.dataset_id

        for merge_spec in datasets:
            right = merge_spec.get("domain_name").lower()

            # TODO: This only handles simple joins for now
            if right in ("relrec", "supp--", "relsub", "co", "sq"):
                raise NotImplementedError("Joins with relationship domains are not supported yet")

            join_type = merge_spec.get("join_type", "INNER")
            # For now we assume pivot columns are always the same in left and right
            pivot_columns = merge_spec.get("match_key", [])

            joined_schema = SqlJoinMerge.perform_join(
                pgi=self.pgi,
                left=self.pgi.schema.get_table(left_id),
                right=self.pgi.schema.get_table(right),
                pivot_left=pivot_columns,
                pivot_right=pivot_columns,
                type=join_type.upper(),
            )
            left_id = joined_schema.name

        return left_id

    def _dataset_exists(self, dataset_name: str) -> bool:
        """Check if a dataset/table exists in the database."""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
            )
        """

        self.pgi.execute_sql(query, (dataset_name.lower(),))

        results = self.pgi.fetch_all()
        if results:
            return results[0]
        return False

    def _is_supp_dataset(self, dataset_id: str) -> bool:
        """Check if a dataset is a SUPP dataset."""
        query = """
            SELECT dataset_is_supp
            FROM public.data_metadata
            WHERE dataset_id = %s
            LIMIT 1
        """

        self.pgi.execute_sql(query, (dataset_id.lower(),))
        result = self.pgi.fetch_one()

        return result["dataset_is_supp"] if result else False

    @staticmethod
    def _get_unsplit_name(
        name: str,
        domain: Union[str, None],
        rdomain: str,
    ) -> str:
        """Get the unsplit name for a dataset."""
        if domain:
            return domain
        if name.startswith("SUPP"):
            return f"SUPP{rdomain}"
        if name.startswith("SQ"):
            return f"SQ{rdomain}"
        return name
