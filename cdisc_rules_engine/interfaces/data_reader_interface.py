import os
import uuid
from typing import List
from cdisc_rules_engine.models.dataset.postgresql_dataset import PostgreSQLDataset


class DataReaderInterface:
    """Interface for reading binary data from different file typs into PostgreSQL dataframe."""

    def __init__(self, connection_pool, bulk_loader):
        self.connection_pool = connection_pool
        self.bulk_loader = bulk_loader

    def read(self, data: bytes, format: str = "csv") -> PostgreSQLDataset:
        """Read data into PostgreSQL."""
        dataset_id = str(uuid.uuid4())

        # write to temporary file
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile(delete=False, suffix=f".{format}") as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        try:
            # load into PostgreSQL
            if format == "csv":
                self.bulk_loader.load_csv(tmp_path, dataset_id)
            elif format == "parquet":
                self.bulk_loader.load_parquet(tmp_path, dataset_id)
            elif format == "xpt":
                self.bulk_loader.load_xpt(tmp_path, dataset_id)

            return PostgreSQLDataset(
                dataset_id=dataset_id, connection_pool=self.connection_pool
            )
        finally:
            os.unlink(tmp_path)
