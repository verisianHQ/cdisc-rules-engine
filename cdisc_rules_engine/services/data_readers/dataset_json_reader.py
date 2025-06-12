import jsonschema.exceptions
import pandas as pd
import dask.dataframe as dd
import os
import json
import jsonschema

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset
import tempfile


class DatasetJSONReader(DataReaderInterface):
    def get_schema(self) -> dict:
        with open(
            os.path.join("resources", "schema", "dataset.schema.json")
        ) as schemajson:
            schema = schemajson.read()
        return json.loads(schema)

    def read_json_file(self, file_path: str) -> dict:
        with open(file_path, "r") as file:
            datasetjson = json.load(file)
        return datasetjson

    def _raw_dataset_from_file(self, file_path) -> pd.DataFrame:
        # Load Dataset-JSON Schema
        schema = self.get_schema()
        datasetjson = self.read_json_file(file_path)

        jsonschema.validate(datasetjson, schema)

        df = pd.DataFrame(
            [item for item in datasetjson.get("rows", [])],
            columns=[item["name"] for item in datasetjson.get("columns", [])],
        )
        return df.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)

    def from_file(self, file_path):
        try:
            df = self._raw_dataset_from_file(file_path)

            database_config = getattr(self, "database_config", None)

            if not database_config:
                raise ValueError("database_config is required for SQLiteDataset")

            records = df.to_dict("records")
            return SQLiteDataset.from_records(records, database_config=database_config)

        except jsonschema.exceptions.ValidationError:
            database_config = getattr(self, "database_config", None)

            if not database_config:
                raise ValueError("database_config is required for SQLiteDataset")

            return SQLiteDataset(database_config=database_config)

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df = self._raw_dataset_from_file(file_path)
        df.to_parquet(temp_file.name)
        return len(df.index), temp_file.name

    def read(self, data):
        pass
