from io import BytesIO
from typing import Union

import pandas as pd
import dask.dataframe as dd
from cdisc_rules_engine.models.dataset.sqlite_dataset import SQLiteDataset

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class ParquetReader(DataReaderInterface):
    def read(self, data):
        df = pd.read_parquet(BytesIO(data), engine="fastparquet", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def _read_sqlite(self, file_path):
        data = pd.read_parquet(file_path, engine="fastparquet", encoding="utf-8")
        data = self._format_floats(data)

        database_config = getattr(self, "database_config", None)
        if not database_config:
            raise ValueError("database_config is required for SQLiteDataset")

        records = data.to_dict("records")
        return SQLiteDataset.from_records(records, database_config=database_config)

    def from_file(self, file_path):
        self._read_sqlite(file_path)

    def _format_floats(
        self, dataframe: Union[pd.DataFrame, dd.DataFrame]
    ) -> Union[pd.DataFrame, dd.DataFrame]:
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)
