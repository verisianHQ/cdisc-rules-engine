from typing import List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset


def load_test_datasets(data_service: PostgresQLDataService, test_datasets: List[TestDataset]):
    # generate timestamp
    timestamp = datetime.now().astimezone()
    for test_dataset in test_datasets:
        # Create schema and table:
        row_dicts = [dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())]
        # force lower_case throughout
        table_name = test_dataset["name"].lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        schema = SqlTableSchema.from_metadata(test_dataset)
        data_service.pgi.create_table(schema)
        data_service.pgi.insert_data(table_name=table_name, data=row_dicts)

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
