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

    # ========== SQLite-specific methods ==========

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
            cursor = conn.cursor()
            return [dict(row) for row in cursor.fetchall()]
        return []

    def fetch_one(self) -> Optional[dict]:
        """Fetch one row from cursor."""
        with self.database_config.get_connection() as conn:
            cursor = conn.cursor()
            row = cursor.fetchone()
            return dict(row) if row else None
        return None

    def _create_dataset_entry(self):
        """Register dataset in metadata table."""
        self.execute_sql(
            """
                INSERT OR IGNORE INTO datasets (dataset_id, table_name)
                VALUES (?, ?)
            """,
            (self.dataset_id, self._table_name),
        )

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

        self.execute_many(
            f"""
            INSERT INTO dataset_records ({columns_str})
            VALUES ({placeholders})
            """,
            values,
        )
        self._length = len(records)
