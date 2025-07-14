import uuid

from abc import ABC, abstractmethod
from typing import List, Optional

from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.config.databases.sqlite_database_config import (
    SQLiteDatabaseConfig,
)


class SQLDatasetBase(DatasetInterface, ABC):
    """Base class for SQL-backed dataset implementations."""

    def __init__(
        self,
        dataset_id: Optional[str] = None,
        database_config: Optional[SQLiteDatabaseConfig] = None,
        columns=None,
        table_name=None,
        length=None,
    ):
        self.dataset_id = dataset_id or str(uuid.uuid4())
        self._columns = columns or []
        self._table_name = table_name or f"dataset_{self.dataset_id.replace('-', '_')}"
        self._length = length
        self.database_config = database_config
        self._create_dataset_entry()

    # ========== Abstract methods that must be implemented by subclasses ==========

    @abstractmethod
    def execute_sql(self, sql_code: str, args: tuple = ()):
        """Execute SQL code with parameters."""
        pass

    @abstractmethod
    def execute_many(self, sql_code: str, data: List[tuple]):
        """Execute SQL code with parameters."""
        pass

    @abstractmethod
    def fetch_all(self) -> List[Optional[dict]]:
        """Fetch all results from cursor."""
        pass

    @abstractmethod
    def fetch_one(self) -> Optional[dict]:
        """Fetch one result from cursor."""
        pass

    @abstractmethod
    def _create_dataset_entry(self):
        """Register dataset in metadata table."""
        pass

    @abstractmethod
    def _insert_records(self, records: List[dict]):
        """Bulk insert records into database."""
        pass

    def _register_columns(self, columns: List[str]):
        """Register columns in metadata table."""

        self.execute_sql(
            "DELETE FROM dataset_columns WHERE dataset_id = ?",
            (self.dataset_id,),
        )

        for idx, col in enumerate(columns):
            placeholders = ", ".join(["?"] * 3)
            self.execute_sql(
                f"""
                INSERT INTO dataset_columns
                (dataset_id, column_name, column_index)
                VALUES ({placeholders})
            """,
                (self.dataset_id, col, idx),
            )

    # ========== Factory methods ==========

    @classmethod
    def from_dict(cls, data: dict, database_config=None, **kwargs) -> "SQLDatasetBase":
        """Create dataset from dictionary."""
        if not database_config:
            raise ValueError("database_config is required")

        dataset = cls(database_config=database_config)

        if not hasattr(dataset, "dataset_id"):
            raise RuntimeError(
                f"Failed to create valid dataset instance of class {cls}"
            )

        records = []
        columns_list = []

        for col, values in data.items():
            if col not in columns_list:
                columns_list.append(col)

            if not isinstance(values, list):
                values = [values]

            for idx, val in enumerate(values):
                if idx >= len(records):
                    records.append({})
                records[idx][col] = val

        if records:
            dataset._insert_records(records)

        if not isinstance(dataset, cls):
            raise RuntimeError(
                f"Dataset is not an instance of {cls}, got {type(dataset)}"
            )

        return dataset

    @classmethod
    def from_records(
        cls, data: List[dict], database_config=None, **kwargs
    ) -> "SQLDatasetBase":
        """Create dataset from list of records."""
        if not database_config:
            raise ValueError("database_config is required")

        provided_columns = kwargs.pop("columns", None)

        dataset = cls(
            database_config=database_config, columns=provided_columns, **kwargs
        )

        if data:
            dataset._insert_records(data)
            if not provided_columns:
                dataset._columns = list(data[0].keys()) if data else []
            if dataset._columns:
                dataset._register_columns(dataset._columns)

        return dataset
