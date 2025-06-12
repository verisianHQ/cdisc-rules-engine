from io import BytesIO

import pandas as pd
from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset
import tempfile

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class XPTReader(DataReaderInterface):
    def read(self, data):
        df = pd.read_sas(BytesIO(data), format="xport", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        dataset = pd.read_sas(file_path, chunksize=20000, encoding="utf-8")
        created = False
        num_rows = 0
        for chunk in dataset:
            chunk = self._format_floats(chunk)
            num_rows += len(chunk)
            if not created:
                chunk.to_parquet(temp_file.name, engine="fastparquet")
                created = True
            else:
                chunk.to_parquet(temp_file.name, engine="fastparquet", append=True)
        return num_rows, temp_file.name

    def from_file(self, file_path):
        # Read the XPT file into a pandas DataFrame first
        df = pd.read_sas(file_path, encoding="utf-8")
        df = self._format_floats(df)

        # Get the database config from the data service
        database_config = getattr(self, "database_config", None)
        if not database_config:
            raise ValueError("database_config is required for SQLiteDataset")

        # Convert to SQLiteDataset
        records = df.to_dict("records")
        dataset = SQLiteDataset.from_records(records, database_config=database_config)

        return dataset

    def _format_floats(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)
