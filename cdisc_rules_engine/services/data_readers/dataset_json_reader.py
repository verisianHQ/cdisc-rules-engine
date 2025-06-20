import pandas as pd
import os
import json
import jsonschema
from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
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

    def _prepare_records_from_dataset_json(self, datasetjson: dict) -> list:
        """Convert Dataset-JSON array format to list of record dictionaries."""
        columns = [item["name"] for item in datasetjson.get("columns", [])]
        rows = datasetjson.get("rows", [])
        records = []

        for row in rows:
            # Create a dictionary for each row
            record = {}
            for idx, col in enumerate(columns):
                if idx < len(row):
                    value = row[idx]
                    if isinstance(value, float):
                        value = round(value, 15)  # 15-rounded float
                    record[col] = value
                else:
                    record[col] = None
            records.append(record)

        return records

    def _raw_dataset_from_file(self, file_path) -> PandasDataset:
        # Load Dataset-JSON Schema
        schema = self.get_schema()
        datasetjson = self.read_json_file(file_path)
        jsonschema.validate(datasetjson, schema)

        records = self._prepare_records_from_dataset_json(datasetjson)
        columns = [item["name"] for item in datasetjson.get("columns", [])]

        df = self.dataset_implementation.from_records(
            records,
            columns=columns,
        )
        return df

    def from_file(self, file_path):
        try:
            df = self._raw_dataset_from_file(file_path)
            if self.dataset_implementation == PandasDataset:
                return PandasDataset(df)
            else:
                return DaskDataset(
                    dd.from_pandas(df, npartitions=4), length=len(df.index)
                )
        except jsonschema.exceptions.ValidationError:
            return PandasDataset(pd.DataFrame())

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        df = self._raw_dataset_from_file(file_path)

        # convert to pd dataframe for parquet export.
        # TODO: implement custom parquet conversion for SQLite.
        records = df.to_dict("records")
        pandas_df = pd.DataFrame(records)
        pandas_df.to_parquet(temp_file.name)

        return len(df), temp_file.name

    def read(self, data):
        pass
