import uuid

from typing import List, Optional

from cdisc_rules_engine.models.dataset.sql_dataset_base import SQLDatasetBase
from cdisc_rules_engine.config.databases.sqlite_database_config import (
    SQLiteDatabaseConfig,
)


class SQLiteDataset(SQLDatasetBase):
    """SQLite-backed dataset implementation."""

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

        if not database_config:
            raise ValueError("database_config is required")

        self.database_config = database_config

        # create dataset entry in metadata table
        self._create_dataset_entry()

    # ========== SQLite cursor methods ==========

    def execute_sql(self, sql_code: str, args: tuple = ()):
        """Execute sql code on cursor."""
        with self.database_config.get_connection() as conn:
            conn.execute(sql_code, args)

    def execute_many(self, sql_code: str, data: List[tuple]):
        """Execute many with sql code on cursor."""
        with self.database_config.get_connection() as conn:
            conn.executemany(sql_code, data)
            conn.commit()

    def fetch_all(self) -> List[Optional[dict]]:
        """Fetch all data from cursor."""
        with self.database_config.get_connection() as conn:
            return [dict(row) for row in conn.cursor().fetchall()]
        return []

    def fetch_one(self) -> Optional[dict]:
        """Fetch one row from cursor."""
        with self.database_config.get_connection() as conn:
            row = conn.cursor().fetchone()
            return dict(row) if row else None
        return None

    # ========== SQLite helper methods ==========

    def _insert_records(self, records: List[dict]):
        """Bulk insert records into dataset."""
        if not records:
            return

        data_columns = list(records[0].keys())
        all_columns = ["dataset_id", "row_num"] + data_columns

        values = []
        for idx, record in enumerate(records):
            row_values = [self.dataset_id, idx] + [
                record.get(col) for col in data_columns
            ]
            values.append(tuple(row_values))

        placeholders = ", ".join(["?" for _ in all_columns])
        columns_str = ", ".join(all_columns)

        column_definitions = ["dataset_id TEXT", "row_num INTEGER"]
        for col in data_columns:
            sample_value = records[0].get(col)
            if isinstance(sample_value, int):
                column_definitions.append(f"{col} INTEGER")
            else:
                column_definitions.append(f"{col} TEXT")

        column_defs_str = ", ".join(column_definitions)

        # Create table and insert in one go
        self.execute_sql("DROP TABLE IF EXISTS dataset_records")
        self.execute_sql(f"CREATE TABLE dataset_records ({column_defs_str})")

        self.execute_many(
            f"INSERT INTO dataset_records ({columns_str}) VALUES ({placeholders})",
            values,
        )
        self._length = len(records)

    def _create_dataset_entry(self):
        """Register dataset in metadata table."""
        self.execute_sql(
            """
                INSERT OR IGNORE INTO datasets (dataset_id, table_name)
                VALUES (?, ?)
            """,
            (self.dataset_id, self._table_name),
        )

    # ========== SQLiteDataset methods ==========

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
