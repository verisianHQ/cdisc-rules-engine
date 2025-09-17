from typing import List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset


class SqlTestDatasetLoader:
    @staticmethod
    def load_test_datasets(data_service: PostgresQLDataService, test_datasets: List[TestDataset]):
        for test_dataset in test_datasets:
            SqlTestDatasetLoader.load_test_dataset(data_service, test_dataset)

    @staticmethod
    def load_test_dataset(data_service: PostgresQLDataService, test_dataset: TestDataset):
        # Create schema and table:
        row_dicts = [dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())]
        # force lower_case throughout
        table_name = test_dataset["name"].lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        schema = SqlTableSchema.from_metadata(test_dataset)
        data_service.pgi.create_table(schema)
        data_service.pgi.insert_data(table_name=table_name, data=row_dicts)

        # TODO INDEX

        data_service.datasets.push(
            SDTMDatasetMetadata(
                file_size=0,
                filename=test_dataset["filename"],
                full_path=test_dataset["filepath"],
                label=test_dataset["label"],
                name=test_dataset["name"],
                record_count=len(row_dicts),
                modification_date=None,
                original_path=None,
                first_record=row_dicts[0],
            )
        )
