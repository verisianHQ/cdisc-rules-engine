import uuid

from abc import ABC, abstractmethod
from typing import List, Optional, Union

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
        """Insert records into the dataset."""
        pass

    @abstractmethod
    def from_dict(cls, data: dict, database_config=None, **kwargs) -> "SQLDatasetBase":
        """Create dataset from dictionary."""
        pass

    @abstractmethod
    def from_records(
        cls, data: List[dict], database_config=None, **kwargs
    ) -> "SQLDatasetBase":
        """Create dataset from list of records."""
        pass

    @property
    def data(self) -> List[Optional[dict]]:
        """Lazy load data when accessed."""
        if self.database_config:
            with self.database_config.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT * FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                    """,
                    (self.dataset_id,),
                )
                return [dict(row) for row in cursor.fetchall()]

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

    @property
    def empty(self):
        """
        Returns whether or not the underlying dataframe is empty
        """
        pass

    @property
    def columns(self):
        """
        Stores the columns of the underlying dataset
        """
        pass

    @classmethod
    def get_series_values(cls, series) -> list:
        """
        Returns the values for a series.
        """
        pass

    def __getitem__(self, item: str):
        """
        Access dataset column by name
        """
        pass

    def __setitem__(self, key: str, data):
        """
        Set value of a dataset column
        """
        pass

    def __len__(self):
        """
        Get length of dataset
        """
        pass

    def __contains__(self, item: str) -> bool:
        """
        Return true if item is in dataset
        """
        pass

    def get(self, column: str, default=None):
        """
        Return column if column is in dataset, else return default
        """
        pass

    def groupby(self, by: List[str], **kwargs):
        """
        Group dataframe by list of columns.
        """
        pass

    def concat(
        self, other: Union["DatasetInterface", List["DatasetInterface"]], **kwargs
    ):
        """
        Concat two datasets
        """
        pass

    def merge(self, other: "DatasetInterface", **kwargs):
        """
        merge two datasets
        """
        pass

    def apply(self, func, **kwargs):
        """
        Apply a function to a dataset
        """
        pass

    def iterrows(self):
        """
        Return iterator over all dataset rows
        """

    @classmethod
    def is_series(cls, data) -> bool:
        """
        Return true if the data is a series compatible with the underlying dataset
        """
        pass

    def convert_to_series(self, data):
        """
        Converts list like data to a series corresponding with the underlying dataset
        """
        pass

    def get_series_from_value(self, value):
        """
        Create a series of a single value
        """
        pass

    def rename(self, index=None, columns=None, inplace=True):
        """
        Rename columns or index labels.
        """
        pass

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """
        Drop specified labels from rows or columns.
        """
        pass

    def melt(
        self,
        id_vars=None,
        value_vars=None,
        var_name=None,
        value_name="value",
        col_level=None,
    ):
        """
        Unpivots a DataFrame from wide format to long format,
        optionally leaving identifier variables set.
        """
        pass

    def set_index(self, keys, **kwargs):
        """
        Wrapper for DataFrame set_index method
        """
        pass

    def filter(self, **kwargs):
        """
        Wrapper for DataFrame filter method
        """
        pass

    def len(self) -> int:
        """
        Return the length of the dataset
        """
        pass

    def assign(self, **kwargs):
        """
        Assign new columns to the dataset.
        This method should return a new instance of the dataset with the new columns added.
        """
        pass

    def copy(self) -> "DatasetInterface":
        """
        Return a new instance of the dataset with the same data
        """
        pass

    def get_error_rows(self, results):
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """
        pass

    def equals(self) -> bool:
        """
        Determine if two datasets are equal
        """
        pass

    def where(cond, other, **kwargs):
        """
        Wrapper for dataframe where function
        """
        pass

    def sort_values(self, by, **kwargs):
        """
        Sort the dataframe by the provided columns
        """
        pass

    def is_column_sorted_within(self, group, column):
        """
        Returns true if the column is sorted within each grouping otherwise false
        """
        pass

    def min(self, *args, **kwargs):
        """
        Return the minimum of the values over the requested axis.
        """
        pass

    def reset_index(self, drop=False, **kwargs):
        """
        Reset the index of the dataset.
        """
        pass

    def fillna(
        self,
        value=None,
        method=None,
        axis=None,
        inplace=False,
        limit=None,
        downcast=None,
    ):
        """
        Fill NA/NaN values using the specified method.
        """
        pass

    def get_grouped_size(self, by, **kwargs):
        """
        Returns a dataframe containing the sizes of each group in
        the dataframe.
        """
        pass

    def to_dict(self, **kwargs) -> dict:
        """
        Convert the dataset to a dictionary.
        """
        pass

    def items(self, **kwargs):
        """
        Convert the dataset to dictionary items.
        Returns a view object displaying a list of (key, value) tuple pairs.
        """
        pass

    def keys(self, **kwargs):
        """
        Returns a view object containing the keys in the dataset dictionary.
        """
        pass

    def values(self, **kwargs):
        """
        Returns a view object containing the values in the dataset dictionary.
        """
        pass
