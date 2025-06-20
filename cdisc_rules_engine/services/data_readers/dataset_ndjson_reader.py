import os
import json
import jsonschema
import tempfile
from typing import List, Any, Tuple

import jsonschema.exceptions
from cdisc_rules_engine.interfaces import DataReaderInterface
from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset
from cdisc_rules_engine.config.databases.sqlite_database_config import (
    SQLiteDatabaseConfig,
)


class DatasetNDJSONReader(DataReaderInterface):
    """Reader for NDJSON files that creates SQLite-backed datasets."""

    def __init__(self, database_path: str = None):
        if not database_path:
            self.database_config = SQLiteDatabaseConfig()
            self.database_config.initialise(in_memory=True)
        else:
            self.database_config = SQLiteDatabaseConfig()
            self.database_config.initialise(database_path=database_path)

        self.dataset_implementation = SQLiteDataset

    def get_schema(self) -> dict:
        """Load the Dataset-JSON schema."""
        with open(
            os.path.join("resources", "schema", "dataset-ndjson-schema.json")
        ) as schema_ndjson:
            schema = schema_ndjson.read()
        return json.loads(schema)

    def read_json_file(self, file_path: str) -> Tuple[dict, List[dict]]:
        """Read NDJSON file and return metadata and data records."""
        with open(file_path, "r") as file:
            lines = file.readlines()
        return json.loads(lines[0]), [json.loads(line) for line in lines[1:]]

    def _prepare_records_from_ndjson(
        self, metadata: dict, data: List[dict]
    ) -> List[dict]:
        """Convert NDJSON data to list of record dictionaries."""
        columns = [item["name"] for item in metadata.get("columns", [])]
        records = []

        for row in data:
            # Create a dictionary for each row
            record = {}
            for idx, col in enumerate(columns):
                if idx < len(row):
                    value = row[idx]
                    # Round floats to 15 decimal places as in the original
                    if isinstance(value, float):
                        value = round(value, 15)
                    record[col] = value
                else:
                    record[col] = None
            records.append(record)

        return records

    def _raw_dataset_from_file(self, file_path: str) -> Tuple[List[dict], List[str]]:
        """Read and validate NDJSON file, return records and columns."""
        schema = self.get_schema()
        metadata_ndjson, data_ndjson = self.read_json_file(file_path)

        jsonschema.validate(metadata_ndjson, schema)

        columns = [item["name"] for item in metadata_ndjson.get("columns", [])]

        records = self._prepare_records_from_ndjson(metadata_ndjson, data_ndjson)

        return records, columns

    def from_file(self, file_path: str) -> SQLiteDataset:
        """Create a SQLiteDataset from an NDJSON file."""
        try:
            records, columns = self._raw_dataset_from_file(file_path)

            dataset = SQLiteDataset.from_records(
                data=records, database_config=self.database_config
            )

            if columns and not dataset.columns:
                dataset.columns = columns

            return dataset

        except jsonschema.exceptions.ValidationError:
            return SQLiteDataset.from_records(
                data=[], database_config=self.database_config
            )

    def to_parquet(self, file_path: str) -> Tuple[int, str]:
        """Convert NDJSON to Parquet format via SQLite."""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")

        dataset = self.from_file(file_path)
        records = dataset.to_dict(orient="records")

        # convert to pandas DataFrame for parquet export
        # TODO: implement a direct SQLite to Parquet converter
        import pandas as pd

        df = pd.DataFrame(records)

        df = df.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)
        df.to_parquet(temp_file.name)

        return len(dataset), temp_file.name

    def read(self, data: Any) -> SQLiteDataset:
        """Read data and return a SQLiteDataset."""
        if isinstance(data, str) and os.path.isfile(data):
            return self.from_file(data)
        elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return SQLiteDataset.from_records(
                data=data, database_config=self.database_config
            )
        else:
            return SQLiteDataset.from_records(
                data=[], database_config=self.database_config
            )

    def close(self):
        """Close database connections and clean up resources."""
        self.database_config.close_all()

        if hasattr(self, "temp_db"):
            try:
                os.unlink(self.temp_db.name)
            except Exception:
                pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
