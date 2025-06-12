import uuid

from abc import ABC, abstractmethod
from typing import List, Union, Dict, Any, Iterator, Tuple

from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface


class SQLDatasetBase(DatasetInterface, ABC):
    """Base class for SQL-backed dataset implementations."""

    def __init__(
        self,
        dataset_id: str = None,
        database_config=None,
        columns=None,
        table_name=None,
        length=None,
    ):
        self.dataset_id = dataset_id or str(uuid.uuid4())
        self._columns = columns or []
        self._table_name = table_name or f"dataset_{self.dataset_id.replace('-', '_')}"
        self._length = length
        self._data = None  # lazy loaded
        self.database_config = database_config
        self._create_dataset_entry()

    # ========== Abstract methods that must be implemented by subclasses ==========

    @abstractmethod
    def execute_sql(self, sql_code: str, args: tuple) -> Any:
        """Execute SQL code with parameters."""
        pass

    @abstractmethod
    def fetch_all(self, cursor=None) -> List[Any]:
        """Fetch all results from cursor."""
        pass

    @abstractmethod
    def fetch_one(self, cursor=None) -> Any:
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

    @abstractmethod
    def _get_json_extract_expr(self, column_name: str) -> str:
        """Get SQL expression for extracting JSON field value."""
        pass

    @abstractmethod
    def _get_json_build_object_expr(self, columns: List[str]) -> str:
        """Get SQL expression for building JSON object."""
        pass

    @abstractmethod
    def _get_json_set_expr(self, column_name: str, value_placeholder: str) -> str:
        """Get SQL expression for setting JSON field."""
        pass

    @abstractmethod
    def _get_json_merge_expr(self, *json_exprs: str) -> str:
        """Get SQL expression for merging JSON objects."""
        pass

    @abstractmethod
    def _get_placeholder(self) -> str:
        """Get SQL parameter placeholder (? for SQLite, %s for PostgreSQL)."""
        pass

    @abstractmethod
    def _parse_json(self, json_data: Any) -> dict:
        """Parse JSON data from database to dict."""
        pass

    @abstractmethod
    def _serialise_json(self, data: dict) -> Any:
        """Serialise dict to JSON for database storage."""
        pass

    @abstractmethod
    def _get_columns(self, column_names: List[str]) -> "SQLDatasetBase":
        """Get multiple columns as new dataset."""
        pass

    @abstractmethod
    def _set_column_value(self, column: str, value: Any, row_idx: int):
        """Set a single cell value."""
        pass

    @abstractmethod
    def _set_column_value_all(self, column: str, value: Any):
        """Set all rows in a column to the same value."""
        pass

    @abstractmethod
    def rename(self, index=None, columns=None, inplace=True):
        """Rename columns."""
        pass

    @abstractmethod
    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """Drop rows or columns."""
        pass

    @abstractmethod
    def _bulk_insert_melt_records(self, records: List[tuple]):
        """Bulk insert records for melt operation."""
        pass

    @abstractmethod
    def _bulk_insert_error_rows(self, rows: List[tuple]):
        """Bulk insert error rows."""
        pass

    @abstractmethod
    def set_index(self, keys, **kwargs):
        """Set index columns (stored as metadata)."""
        pass

    @abstractmethod
    def _bulk_insert_where_rows(self, rows: List[tuple]):
        """Bulk insert filtered rows."""
        pass

    @abstractmethod
    def concat(
        self, other: Union["SQLDatasetBase", List["SQLDatasetBase"]], axis=0, **kwargs
    ):
        """Concatenate datasets."""
        pass

    @abstractmethod
    def merge(self, other: type["SQLDatasetBase"], on=None, how="inner", **kwargs):
        """Merge datasets using sql join."""
        pass

    @abstractmethod
    def _build_order_clause(self, columns: List[str], ascending: bool) -> str:
        """Build ORDER BY clause for sorting."""
        pass

    @abstractmethod
    def is_column_sorted_within(
        self, group: Union[str, List[str]], column: str
    ) -> bool:
        """Check if column is sorted within groups."""
        pass

    @abstractmethod
    def min(self, axis=0, skipna=True, **kwargs):
        """Get minimum values."""
        pass

    @abstractmethod
    def reset_index(self, drop=False, **kwargs):
        """Reset row numbers to sequential."""
        pass

    @abstractmethod
    def fillna(
        self,
        value=None,
        method=None,
        axis=None,
        inplace=False,
        limit=None,
        downcast=None,
    ):
        """Fill null values."""
        pass

    # ========== Common properties ==========

    @property
    def data(self):
        """Lazy load data when accessed."""
        if self._data is None:
            cursor = self.execute_sql(
                f"""
                    SELECT data FROM dataset_records
                    WHERE dataset_id = {self._get_placeholder()}
                    ORDER BY row_num
                """,
                (self.dataset_id,),
            )
            self._data = [
                self._parse_json(row["data"]) for row in self.fetch_all(cursor)
            ]
        return self._data

    @property
    def empty(self):
        """Check if dataset has no records."""
        return len(self) == 0

    @property
    def columns(self):
        """Get column names from metadata or data."""
        if not self._columns:
            # first try metadata table
            cursor = self.execute_sql(
                f"""
                SELECT column_name 
                FROM dataset_columns 
                WHERE dataset_id = {self._get_placeholder()}
                ORDER BY column_index
            """,
                (self.dataset_id,),
            )
            cols = [row["column_name"] for row in self.fetch_all(cursor)]

            if not cols:
                # fallback to extracting from first record
                cursor = self.execute_sql(
                    f"""
                    SELECT data FROM dataset_records
                    WHERE dataset_id = {self._get_placeholder()}
                    ORDER BY row_num LIMIT 1
                """,
                    (self.dataset_id,),
                )
                result = self.fetch_one(cursor)
                if result and result["data"]:
                    cols = list(self._parse_json(result["data"]).keys())
                    # store columns for future use
                    self._register_columns(cols)

            self._columns = cols
        return self._columns

    @columns.setter
    def columns(self, columns):
        """Set column names and update metadata."""
        self._columns = columns
        if self.database_config:
            self._register_columns(columns)

    def _register_columns(self, columns: List[str]):
        """Register columns in metadata table."""
        # clear existing columns
        self.execute_sql(
            f"DELETE FROM dataset_columns WHERE dataset_id = {self._get_placeholder()}",
            (self.dataset_id,),
        )
        # insert new columns
        for idx, col in enumerate(columns):
            placeholders = ", ".join([self._get_placeholder()] * 3)
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
    def from_dict(cls, data: dict, database_config=None, **kwargs):
        """Create dataset from dictionary."""
        if not database_config:
            raise ValueError("database_config is required")

        dataset = cls(database_config=database_config)

        records = []
        columns = set()
        for col, values in data.items():
            columns.add(col)
            for idx, val in enumerate(values):
                if idx >= len(records):
                    records.append({})
                records[idx][col] = val

        dataset._insert_records(records)
        dataset._columns = list(columns)
        dataset._register_columns(dataset._columns)

        return dataset

    @classmethod
    def from_records(cls, data: List[dict], database_config=None, **kwargs):
        """Create dataset from list of records."""
        if not database_config:
            raise ValueError("database_config is required")

        dataset = cls(database_config=database_config)

        if data:
            dataset._insert_records(data)
            dataset._columns = list(data[0].keys()) if data else []
            if dataset._columns:
                dataset._register_columns(dataset._columns)

        return dataset

    # ========== Common class methods ==========

    @classmethod
    def get_series_values(cls, series) -> list:
        """Extract values from series-like object."""
        if isinstance(series, list):
            return series
        elif hasattr(series, "tolist"):
            return series.tolist()
        elif hasattr(series, "values"):
            return list(series.values)
        return list(series)

    @classmethod
    def is_series(cls, data) -> bool:
        """Check if data is series-like."""
        return isinstance(data, (list, tuple)) or hasattr(data, "__iter__")

    @classmethod
    def cartesian_product(
        cls, left: "SQLDatasetBase", right: "SQLDatasetBase"
    ) -> "SQLDatasetBase":
        """Create cartesian product of two datasets."""
        return left.merge(right, how="cross")

    # ========== Magic methods ==========

    def __getitem__(self, item: Union[str, List[str], slice, int]):
        """Implement column/row selection."""
        if isinstance(item, str):
            return self._get_column(item)
        elif isinstance(item, list):
            return self._get_columns(item)
        elif isinstance(item, slice):
            return self._get_rows(item)
        elif isinstance(item, int):
            return self._get_row(item)
        else:
            raise TypeError(f"Invalid argument type: {type(item)}")

    def __setitem__(self, key: str, value):
        """Set column values."""
        if not self.database_config:
            return

        if isinstance(value, (list, tuple)):
            # set column from list of values
            for idx, val in enumerate(value):
                self._set_column_value(key, val, idx)
        else:
            # set all rows to single value
            self._set_column_value_all(key, value)

        # update columns if new
        if key not in self._columns:
            self._columns.append(key)
            self._register_columns(self._columns)

    def __len__(self):
        """Get number of rows."""
        if self._length is None:
            cursor = self.execute_sql(
                f"""
                SELECT COUNT(*) FROM dataset_records
                WHERE dataset_id = {self._get_placeholder()}
            """,
                (self.dataset_id,),
            )
            result = self.fetch_one(cursor)
            self._length = (
                result[0] if isinstance(result, tuple) else result.get("count", 0)
            )
        return self._length or 0

    def __contains__(self, item: str) -> bool:
        """Check if column exists."""
        return item in self.columns

    # ========== Data access methods ==========

    def _get_column(self, column_name: str) -> List[Any]:
        """Get single column as list."""
        json_extract = self._get_json_extract_expr(column_name)
        cursor = self.execute_sql(
            f"""
            SELECT {json_extract} as value
            FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()}
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )
        return [row["value"] for row in self.fetch_all(cursor)]

    def _get_rows(self, slice_obj: slice) -> "SQLDatasetBase":
        """Get rows by slice."""
        start = slice_obj.start or 0
        stop = slice_obj.stop or len(self)

        new_dataset_id = str(uuid.uuid4())
        placeholders = ", ".join([self._get_placeholder()] * 5)
        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT {placeholders.split(', ')[0]}, row_num - {placeholders.split(', ')[1]}, data
            FROM dataset_records
            WHERE dataset_id = {placeholders.split(', ')[2]} 
              AND row_num >= {placeholders.split(', ')[3]} 
              AND row_num < {placeholders.split(', ')[4]}
            ORDER BY row_num
        """,
            (new_dataset_id, start, self.dataset_id, start, stop),
        )

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns,
        )

    def _get_row(self, row_idx: int) -> dict:
        """Get single row as dict."""
        cursor = self.execute_sql(
            f"""
            SELECT data FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()} AND row_num = {self._get_placeholder()}
        """,
            (self.dataset_id, row_idx),
        )
        result = self.fetch_one(cursor)
        return self._parse_json(result["data"]) if result else {}

    def get(self, column: Union[str, List[str]], default=None):
        """Get column with default if not exists."""
        if isinstance(column, list):
            # check all columns exist
            if all(col in self.columns for col in column):
                return self._get_columns(column)
            return default
        else:
            if column in self.columns:
                return self._get_column(column)
            return default

    # ========== Iteration methods ==========

    def _iterate_cursor(self, cursor):
        """Iterate through cursor results - override if needed."""
        return self.fetch_all(cursor)

    def iterrows(self) -> Iterator[Tuple[int, dict]]:
        """Iterate over rows."""
        cursor = self.execute_sql(
            f"""
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()}
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        for row in self._iterate_cursor(cursor):
            yield (row["row_num"], self._parse_json(row["data"]))

    # ========== Transformation methods ==========

    def convert_to_series(self, data) -> List[Any]:
        """Convert data to list."""
        if isinstance(data, list):
            return data
        elif hasattr(data, "tolist"):
            return data.tolist()
        elif hasattr(data, "__iter__") and not isinstance(data, (str, dict)):
            return list(data)
        else:
            return [data] * len(self)

    def get_series_from_value(self, value) -> List[Any]:
        """Create series of repeated value."""
        return [value] * len(self)

    def melt(
        self,
        id_vars=None,
        value_vars=None,
        var_name=None,
        value_name="value",
        col_level=None,
    ):
        """Unpivot dataset from wide to long format."""
        if not self.database_config:
            return self.__class__()

        id_vars = id_vars or []
        value_vars = value_vars or [c for c in self.columns if c not in id_vars]
        var_name = var_name or "variable"

        new_dataset_id = str(uuid.uuid4())

        # create unpivoted records
        insert_values = []
        row_num = 0

        for orig_row in self.iterrows():
            row_data = orig_row[1]
            id_data = {k: row_data.get(k) for k in id_vars}

            for var in value_vars:
                if var in row_data:
                    new_row = id_data.copy()
                    new_row[var_name] = var
                    new_row[value_name] = row_data[var]
                    insert_values.append(
                        (new_dataset_id, row_num, self._serialise_json(new_row))
                    )
                    row_num += 1

        # bulk insert - handled by subclass
        self._bulk_insert_melt_records(insert_values)

        # new columns
        new_columns = id_vars + [var_name, value_name]

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=new_columns,
            length=row_num,
        )

    def filter(self, items=None, like=None, regex=None, axis=None):
        """Filter columns by criteria."""
        if axis in (1, "columns"):
            filtered_cols = self._columns.copy()

            if items is not None:
                filtered_cols = [c for c in filtered_cols if c in items]
            elif like is not None:
                filtered_cols = [c for c in filtered_cols if like in c]
            elif regex is not None:
                import re

                pattern = re.compile(regex)
                filtered_cols = [c for c in filtered_cols if pattern.search(c)]

            return self._get_columns(filtered_cols)

        return self

    def len(self) -> int:
        """Get row count."""
        return len(self)

    def copy(self) -> "SQLDatasetBase":
        """Create deep copy of dataset."""
        new_dataset_id = str(uuid.uuid4())

        # copy all records
        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT {self._get_placeholder()}, row_num, data
            FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()}
        """,
            (new_dataset_id, self.dataset_id),
        )

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns.copy(),
            length=self._length,
        )

    def get_error_rows(self, results) -> "SQLDatasetBase":
        """Get rows where results is true."""
        new_dataset_id = str(uuid.uuid4())

        # filter rows where result is true
        error_rows = []
        for idx, (row_num, row_data) in enumerate(self.iterrows()):
            if idx < len(results) and results[idx]:
                error_rows.append(
                    (new_dataset_id, len(error_rows), self._serialise_json(row_data))
                )

        if error_rows:
            self._bulk_insert_error_rows(error_rows[:1000])  # limit to 1000

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns,
            length=min(len(error_rows), 1000),
        )

    def equals(self, other: "SQLDatasetBase") -> bool:
        """Check if datasets are equal."""
        if len(self) != len(other) or self.columns != other.columns:
            return False

        if self.database_config:
            # compare data row by row
            for (_, row1), (_, row2) in zip(self.iterrows(), other.iterrows()):
                if row1 != row2:
                    return False
            return True

        return False

    def where(self, cond, other=None, **kwargs):
        """Filter rows by condition."""
        new_dataset_id = str(uuid.uuid4())

        # if cond is string, use as sql where clause
        if isinstance(cond, str):
            self.execute_sql(
                f"""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                SELECT {self._get_placeholder()}, 
                       ROW_NUMBER() OVER (ORDER BY row_num) - 1,
                       data
                FROM dataset_records
                WHERE dataset_id = {self._get_placeholder()} AND ({cond})
            """,
                (new_dataset_id, self.dataset_id),
            )
        else:
            # cond is list of booleans
            filtered_rows = []
            for idx, (row_num, row_data) in enumerate(self.iterrows()):
                if idx < len(cond) and cond[idx]:
                    filtered_rows.append(
                        (
                            new_dataset_id,
                            len(filtered_rows),
                            self._serialise_json(row_data),
                        )
                    )

            if filtered_rows:
                self._bulk_insert_where_rows(filtered_rows)

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns,
        )

    def apply(self, func, axis=0, **kwargs):
        """Apply function to dataset."""
        if self.database_config:
            if axis == 1:  # row-wise
                # fetch all data and apply function
                results = []
                for row in self.iterrows():
                    results.append(func(row[1]))

                # create new dataset with results
                new_dataset = self.__class__(database_config=self.database_config)
                if isinstance(results[0], dict):
                    new_dataset._insert_records(results)
                else:
                    new_dataset._insert_records([{"result": r} for r in results])
                return new_dataset
            else:  # column-wise
                # apply to each column
                result_dict = {}
                for col in self.columns:
                    col_data = self._get_column(col)
                    result_dict[col] = func(col_data)

                return result_dict

        return {}

    def sort_values(self, by: Union[str, List[str]], ascending=True, **kwargs):
        """Sort dataset by columns."""
        if isinstance(by, str):
            by = [by]

        new_dataset_id = str(uuid.uuid4())

        # build order by clause
        order_clause = self._build_order_clause(by, ascending)

        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT {self._get_placeholder()},
                   ROW_NUMBER() OVER (ORDER BY {order_clause}) - 1,
                   data
            FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()}
            ORDER BY {order_clause}
        """,
            (new_dataset_id, self.dataset_id),
        )

        return self.__class__(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns,
            length=self._length,
        )

    def get_grouped_size(self, by: Union[str, List[str]], **kwargs):
        """Get size of each group."""
        if isinstance(by, str):
            by = [by]

        group_cols = ", ".join(
            [f"{self._get_json_extract_expr(col)} as {col}" for col in by]
        )
        group_by = ", ".join([self._get_json_extract_expr(col) for col in by])

        cursor = self.execute_sql(
            f"""
            SELECT {group_cols}, COUNT(*) as size
            FROM dataset_records
            WHERE dataset_id = {self._get_placeholder()}
            GROUP BY {group_by}
        """,
            (self.dataset_id,),
        )

        # return as new dataset
        records = []
        for row in self.fetch_all(cursor):
            record = {col: row[col] for col in by}
            record["size"] = row["size"]
            records.append(record)

        return self.__class__.from_records(
            records, database_config=self.database_config
        )

    def to_dict(self, orient="records"):
        """Export to dictionary format."""
        if orient == "records":
            cursor = self.execute_sql(
                f"""
                SELECT data 
                FROM dataset_records 
                WHERE dataset_id = {self._get_placeholder()} 
                ORDER BY row_num
            """,
                (self.dataset_id,),
            )
            return [self._parse_json(row["data"]) for row in self.fetch_all(cursor)]

        elif orient == "list" or orient == "series":
            result = {}
            for col in self.columns:
                result[col] = self._get_column(col)
            return result

        elif orient == "split":
            return {
                "index": list(range(len(self))),
                "columns": self.columns,
                "data": [list(row[1].values()) for row in self.iterrows()],
            }

        elif orient == "index":
            return {row[0]: row[1] for row in self.iterrows()}

        return {} if orient != "records" else []

    # ========== Groupby operations ==========

    def groupby(self, by: Union[str, List[str]], **kwargs):
        """Group by columns."""
        if isinstance(by, str):
            by = [by]

        # return a GroupedDataset-like object
        return GroupedSQLOperations(self, by, **kwargs)


class GroupedSQLOperations:
    """Helper for grouped operations on SQL datasets."""

    def __init__(self, dataset: SQLDatasetBase, groupby_cols: List[str], **kwargs):
        self.dataset = dataset
        self.groupby_cols = groupby_cols
        self.kwargs = kwargs

    def agg(self, func_dict: Dict[str, Union[str, List[str]]]):
        """Aggregate using multiple functions."""
        if not self.dataset.database_config:
            return self.dataset.__class__()

        # let the child dataset build the aggregation query
        return self.dataset._execute_aggregation(self.groupby_cols, func_dict)

    def sum(self):
        """Sum numeric columns."""
        numeric_cols = self._get_numeric_columns()
        return self.agg({col: "sum" for col in numeric_cols})

    def mean(self):
        """Average numeric columns."""
        numeric_cols = self._get_numeric_columns()
        return self.agg({col: "mean" for col in numeric_cols})

    def size(self):
        """Get size of each group."""
        return self.dataset.get_grouped_size(self.groupby_cols)

    def _get_numeric_columns(self):
        """Identify numeric columns."""
        numeric_cols = []
        for col in self.dataset.columns:
            if col not in self.groupby_cols:
                # sample first value to check type
                sample = self.dataset._get_column(col)
                if sample and isinstance(sample[0], (int, float, str)):
                    try:
                        float(sample[0])
                        numeric_cols.append(col)
                    except (ValueError, TypeError):
                        pass
        return numeric_cols
