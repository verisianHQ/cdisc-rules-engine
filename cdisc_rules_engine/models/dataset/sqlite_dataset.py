import json
import numpy as np
import uuid

from math import isnan
from typing import List, Dict, Any, Union, Optional, Tuple

from cdisc_rules_engine.models.dataset.sql_dataset_base import SQLDatasetBase
from cdisc_rules_engine.config.databases.sqlite_database_config import (
    SQLiteDatabaseConfig,
)


class SQLiteDataset(SQLDatasetBase):
    """SQLite-backed dataset implementation."""

    def __init__(
        self,
        dataset_id: str = None,
        database_config: SQLiteDatabaseConfig = None,
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
        if not self.database_config:
            raise ValueError("database_config is required")

        # create dataset entry in metadata table
        self._create_dataset_entry()

    # ========== SQLite-specific methods ==========

    def execute_sql(self, sql_code: str, args: tuple = ()) -> Any:
        """Execute sql code on cursor."""
        with self.database_config.get_connection() as conn:
            return conn.execute(sql_code, args)

    def execute_many(self, sql_code: str, data: List[tuple]):
        """Execute many with sql code on cursor."""
        with self.database_config.get_connection() as conn:
            conn.executemany(sql_code, data)
            conn.commit()

    def fetch_all(self, cursor=None) -> List[Any]:
        """Fetch all data from cursor."""
        if cursor:
            return [dict(row) for row in cursor.fetchall()]
        return []

    def fetch_one(self, cursor=None) -> Any:
        """Fetch one row from cursor."""
        if cursor:
            row = cursor.fetchone()
            return dict(row) if row else None
        return None

    def __getitem__(self, item: Union[str, List[str]]):
        """Get column(s) from dataset."""
        if isinstance(item, str):
            cursor = self.execute_sql(
                """
                SELECT json_extract(data, ?) as value
                FROM dataset_records
                WHERE dataset_id = ?
                ORDER BY row_num
            """,
                (f"$.{item}", self.dataset_id),
            )
            
            return [row["value"] for row in self.fetch_all(cursor)]
        
        elif isinstance(item, list):
            return self._columns(item)
        
        else:
            raise TypeError(f"Unsupported key type: {type(item)}")

    def __contains__(self, item: str) -> bool:
        """Check if column exists in dataset."""
        return item in self._columns

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
        """Bulk insert records into database."""
        if not records:
            return

        values = [
            (self.dataset_id, idx, json.dumps(record))
            for idx, record in enumerate(records)
        ]

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            values,
        )
        self._length = len(records)

    def _get_json_extract_expr(self, column_name: str) -> str:
        """Get SQL expression for extracting JSON field value."""
        return f"json_extract(data, '$.{column_name}')"

    def _get_json_build_object_expr(self, columns: List[str]) -> str:
        """Get SQL expression for building JSON object."""
        pairs = []
        for col in columns:
            pairs.append(f"'{col}', json_extract(data, '$.{col}')")
        return f"json_object({', '.join(pairs)})"

    def _get_json_set_expr(self, column_name: str, value_placeholder: str) -> str:
        """Get SQL expression for setting JSON field."""
        return f"json_set(data, '$.{column_name}', json({value_placeholder}))"

    def _get_json_merge_expr(self, *json_exprs: str) -> str:
        """Get SQL expression for merging JSON objects."""
        result = json_exprs[0]
        for expr in json_exprs[1:]:
            result = f"json_patch({result}, {expr})"
        return result

    def _get_placeholder(self) -> str:
        """SQLite uses ? for placeholders."""
        return "?"

    def _parse_json(self, json_data: Any) -> dict:
        """Parse JSON string to dict."""
        return json.loads(json_data) if isinstance(json_data, str) else json_data

    def _serialise_json(self, data: dict) -> str:
        """Serialise dict to JSON for SQLite."""
        return json.dumps(data)

    def _set_column_value(self, column: str, value: Any, row_idx: int):
        """Set a single cell value."""
        self.execute_sql(
            """
            UPDATE dataset_records
            SET data = json_set(data, ?, json(?))
            WHERE dataset_id = ? AND row_num = ?
        """,
            (f"$.{column}", json.dumps(value), self.dataset_id, row_idx),
        )

    def _set_column_value_all(self, column: str, value: Any):
        """Set all rows in a column to the same value."""
        self.execute_sql(
            """
            UPDATE dataset_records
            SET data = json_set(data, ?, json(?))
            WHERE dataset_id = ?
        """,
            (f"$.{column}", json.dumps(value), self.dataset_id),
        )

    def _get_columns(self, column_names: List[str]) -> "SQLiteDataset":
        """Get multiple columns as new dataset."""
        new_dataset_id = str(uuid.uuid4())

        # Build JSON object with selected columns
        json_build = self._get_json_build_object_expr(column_names)

        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, row_num, {json_build}
            FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (new_dataset_id, self.dataset_id),
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=column_names,
        )

    def rename(self, index=None, columns=None, inplace=True):
        """Rename columns."""
        if columns:
            with self.database_config.get_connection() as conn:
                for old_name, new_name in columns.items():
                    # Update each record's JSON
                    cursor = conn.execute(
                        """
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """,
                        (self.dataset_id,),
                    )

                    for row in cursor.fetchall():
                        data = json.loads(row["data"])
                        if old_name in data:
                            data[new_name] = data.pop(old_name)
                            conn.execute(
                                """
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """,
                                (json.dumps(data), row["record_id"]),
                            )

                conn.commit()

            # update columns list
            self._columns = [columns.get(c, c) for c in self._columns]
            self._register_columns(self._columns)

        return self if inplace else self.copy()

    def set_index(self, keys, **kwargs):
        """Set index columns (stored as metadata)."""
        if isinstance(keys, str):
            keys = [keys]

        with self.database_config.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT metadata FROM datasets WHERE dataset_id = ?
            """,
                (self.dataset_id,),
            )

            row = cursor.fetchone()
            metadata = json.loads(row["metadata"] or "{}") if row else {}
            metadata["index_columns"] = keys

            conn.execute(
                """
                UPDATE datasets
                SET metadata = ?
                WHERE dataset_id = ?
            """,
                (json.dumps(metadata), self.dataset_id),
            )
            conn.commit()

        return self

    def _bulk_insert_melt_records(self, records: List[tuple]):
        """Bulk insert records for melt operation."""
        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

    def _bulk_insert_error_rows(self, rows: List[tuple]):
        """Bulk insert error rows."""
        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            rows,
        )

    def _bulk_insert_where_rows(self, rows: List[tuple]):
        """Bulk insert filtered rows."""
        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            rows,
        )

    # ========== Factory methods ==========

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """Drop rows or columns."""
        if axis == 1 or columns:  # drop columns
            cols_to_drop = columns or labels
            if isinstance(cols_to_drop, str):
                cols_to_drop = [cols_to_drop]

            # remove from data
            with self.database_config.get_connection() as conn:
                for col in cols_to_drop:
                    if errors == "raise" and col not in self._columns:
                        raise KeyError(f"Column '{col}' not found")

                    # Update each record's JSON
                    cursor = conn.execute(
                        """
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """,
                        (self.dataset_id,),
                    )

                    for row in cursor.fetchall():
                        data = json.loads(row["data"])
                        if col in data:
                            del data[col]
                            conn.execute(
                                """
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """,
                                (json.dumps(data), row["record_id"]),
                            )
                conn.commit()

            # update columns
            self._columns = [c for c in self._columns if c not in cols_to_drop]
            self._register_columns(self._columns)

        else:  # drop rows
            if isinstance(labels, int):
                labels = [labels]

            with self.database_config.get_connection() as conn:
                for label in labels:
                    conn.execute(
                        """
                        DELETE FROM dataset_records
                        WHERE dataset_id = ? AND row_num = ?
                    """,
                        (self.dataset_id, label),
                    )

                # reindex remaining rows
                conn.execute(
                    """
                    UPDATE dataset_records
                    SET row_num = (
                        SELECT COUNT(*) 
                        FROM dataset_records dr2 
                        WHERE dr2.dataset_id = dataset_records.dataset_id 
                          AND dr2.row_num < dataset_records.row_num
                    )
                    WHERE dataset_id = ?
                """,
                    (self.dataset_id,),
                )
                conn.commit()

            self._length = None  # reset cached length

    def concat(
        self, other: Union["SQLiteDataset", List["SQLiteDataset"]], axis=0, **kwargs
    ):
        """Concatenate datasets."""
        if axis == 0:  # vertical concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())

            with self.database_config.get_connection() as conn:
                # copy self first
                conn.execute(
                    """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT ?, row_num, data
                    FROM dataset_records
                    WHERE dataset_id = ?
                """,
                    (new_dataset_id, self.dataset_id),
                )

                # append others
                offset = len(self)
                for ds in datasets:
                    conn.execute(
                        """
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT ?, row_num + ?, data
                        FROM dataset_records
                        WHERE dataset_id = ?
                    """,
                        (new_dataset_id, offset, ds.dataset_id),
                    )
                    offset += len(ds)
                conn.commit()

            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=self._columns,
            )
        else:  # horizontal concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())

            # Build a query that merges JSON objects
            with self.database_config.get_connection() as conn:
                # First, create temp table with all data
                temp_table = f"temp_concat_{new_dataset_id.replace('-', '_')}"
                conn.execute(
                    f"""
                    CREATE TEMP TABLE {temp_table} AS
                    SELECT row_num, data as data0 FROM dataset_records WHERE dataset_id = ?
                """,
                    (self.dataset_id,),
                )

                # Add columns from other datasets
                for i, ds in enumerate(datasets):
                    conn.execute(
                        f"""
                        ALTER TABLE {temp_table}
                        ADD COLUMN data{i+1} TEXT
                    """
                    )

                    conn.execute(
                        f"""
                        UPDATE {temp_table}
                        SET data{i+1} = (
                            SELECT data FROM dataset_records 
                            WHERE dataset_id = ? AND row_num = {temp_table}.row_num
                        )
                    """,
                        (ds.dataset_id,),
                    )

                # Merge JSON objects
                merge_expr = "json(data0)"
                for i in range(len(datasets)):
                    merge_expr = f"json_patch({merge_expr}, json(data{i+1}))"

                conn.execute(
                    f"""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT ?, row_num, {merge_expr}
                    FROM {temp_table}
                """,
                    (new_dataset_id,),
                )

                conn.execute(f"DROP TABLE {temp_table}")
                conn.commit()

            # combine columns
            all_columns = list(self._columns)
            for ds in datasets:
                all_columns.extend([c for c in ds.columns if c not in all_columns])

            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=all_columns,
            )

    def merge(self, other: type["SQLDatasetBase"], on=None, how="inner", **kwargs):
        """Merge datasets using sql join."""
        join_type_map = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "outer": "LEFT JOIN",  # SQLite doesn't have FULL OUTER, simulate with UNION
            "cross": "CROSS JOIN",
        }

        join_type = join_type_map.get(how, "INNER JOIN")
        new_dataset_id = str(uuid.uuid4())

        if on:
            if isinstance(on, str):
                on = [on]
            join_conditions = " AND ".join(
                [
                    f"json_extract(a.data, '$.{col}') = json_extract(b.data, '$.{col}')"
                    for col in on
                ]
            )
        else:
            join_conditions = "1=1"

        with self.database_config.get_connection() as conn:
            if how == "outer":
                # Simulate FULL OUTER JOIN with UNION
                conn.execute(
                    f"""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT 
                        ?,
                        ROW_NUMBER() OVER (ORDER BY row_num) - 1,
                        json_patch(
                            COALESCE(json(a_data), '{{}}'), 
                            COALESCE(json(b_data), '{{}}')
                        )
                    FROM (
                        SELECT a.row_num, a.data as a_data, b.data as b_data
                        FROM dataset_records a
                        LEFT JOIN dataset_records b 
                            ON {join_conditions} AND b.dataset_id = ?
                        WHERE a.dataset_id = ?
                        
                        UNION
                        
                        SELECT b.row_num, a.data as a_data, b.data as b_data
                        FROM dataset_records b
                        LEFT JOIN dataset_records a 
                            ON {join_conditions} AND a.dataset_id = ?
                        WHERE b.dataset_id = ?
                    ) merged
                """,
                    (
                        new_dataset_id,
                        other.dataset_id,
                        self.dataset_id,
                        self.dataset_id,
                        other.dataset_id,
                    ),
                )
            else:
                conn.execute(
                    f"""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT 
                        ?,
                        ROW_NUMBER() OVER (ORDER BY a.row_num, b.row_num) - 1,
                        json_patch(json(a.data), json(b.data))
                    FROM dataset_records a
                    {join_type} dataset_records b 
                        ON {join_conditions} AND b.dataset_id = ?
                    WHERE a.dataset_id = ?
                """,
                    (new_dataset_id, other.dataset_id, self.dataset_id),
                )
            conn.commit()

        # combine columns
        merged_columns = list(self._columns)
        merged_columns.extend([c for c in other.columns if c not in merged_columns])

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=merged_columns,
        )

    def _build_order_clause(self, columns: List[str], ascending: bool) -> str:
        """Build ORDER BY clause with SQLite type casting."""
        direction = "ASC" if ascending else "DESC"
        order_parts = []
        for col in columns:
            # Cast to REAL for numeric sorting, fallback to text
            order_parts.append(
                f"CAST(json_extract(data, '$.{col}') AS REAL) {direction}, "
                f"json_extract(data, '$.{col}') {direction}"
            )
        return ", ".join(order_parts)

    def is_column_sorted_within(
        self, group: Union[str, List[str]], column: str
    ) -> bool:
        """Check if column is sorted within groups."""
        if isinstance(group, str):
            group = [group]

        # Build partition clause
        partition_cols = ", ".join([f"json_extract(data, '$.{g}')" for g in group])

        cursor = self.execute_sql(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT json_extract(data, '$.{column}') as col_val,
                           LAG(json_extract(data, '$.{column}')) OVER (
                               PARTITION BY {partition_cols}
                               ORDER BY row_num
                           ) as prev_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                ) t
                WHERE prev_val IS NOT NULL 
                  AND CAST(prev_val AS REAL) > CAST(col_val AS REAL)
            )
        """,
            (self.dataset_id,),
        )

        return not cursor.fetchone()[0]

    def min(self, axis=0, skipna=True, **kwargs):
        """Get minimum values."""
        if axis == 0:  # column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT MIN(CAST(json_extract(data, ?) AS REAL)) as min_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """,
                    (f"$.{col}", self.dataset_id, f"$.{col}"),
                )
                result[col] = cursor.fetchone()["min_val"]
            return result
        else:  # row-wise
            results = []
            for row_num, row_data in self.iterrows():
                numeric_vals = [
                    v for v in row_data.values() if isinstance(v, (int, float))
                ]
                results.append(min(numeric_vals) if numeric_vals else None)
            return results

    def reset_index(self, drop=False, **kwargs):
        """Reset row numbers to sequential."""
        with self.database_config.get_connection() as conn:
            if not drop:
                # save current row_num as index column
                cursor = conn.execute(
                    """
                    SELECT record_id, row_num, data FROM dataset_records
                    WHERE dataset_id = ?
                """,
                    (self.dataset_id,),
                )

                for row in cursor.fetchall():
                    data = json.loads(row["data"])
                    data["index"] = row["row_num"]
                    conn.execute(
                        """
                        UPDATE dataset_records
                        SET data = ?
                        WHERE record_id = ?
                    """,
                        (json.dumps(data), row["record_id"]),
                    )

                if "index" not in self._columns:
                    self._columns.insert(0, "index")
                    self._register_columns(self._columns)

            # resequence row numbers
            conn.execute(
                """
                UPDATE dataset_records
                SET row_num = (
                    SELECT COUNT(*) 
                    FROM dataset_records dr2 
                    WHERE dr2.dataset_id = dataset_records.dataset_id 
                      AND dr2.row_num < dataset_records.row_num
                )
                WHERE dataset_id = ?
            """,
                (self.dataset_id,),
            )
            conn.commit()

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
        dataset = self if inplace else self.copy()

        with self.database_config.get_connection() as conn:
            if value is not None:
                # fill with specific value
                for col in dataset.columns:
                    cursor = conn.execute(
                        """
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """,
                        (dataset.dataset_id,),
                    )

                    for row in cursor.fetchall():
                        data = json.loads(row["data"])
                        if col not in data or data[col] is None:
                            data[col] = value
                            conn.execute(
                                """
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """,
                                (json.dumps(data), row["record_id"]),
                            )

            elif method == "ffill":
                # forward fill
                for col in dataset.columns:
                    last_valid = None
                    cursor = conn.execute(
                        """
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                        ORDER BY row_num
                    """,
                        (dataset.dataset_id,),
                    )

                    for row in cursor.fetchall():
                        data = json.loads(row["data"])
                        if col in data and data[col] is not None:
                            last_valid = data[col]
                        elif last_valid is not None:
                            data[col] = last_valid
                            conn.execute(
                                """
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """,
                                (json.dumps(data), row["record_id"]),
                            )

            conn.commit()

        return dataset

    def _execute_aggregation(
        self, groupby_cols: List[str], func_dict: Dict[str, Union[str, List[str]]]
    ):
        """Execute aggregation query."""
        # build aggregation expressions
        agg_exprs = []
        result_cols = list(groupby_cols)

        for col, funcs in func_dict.items():
            if isinstance(funcs, str):
                funcs = [funcs]

            for func in funcs:
                if func == "sum":
                    agg_exprs.append(
                        f"SUM(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_sum"
                    )
                    result_cols.append(f"{col}_sum")
                elif func == "mean" or func == "avg":
                    agg_exprs.append(
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_mean"
                    )
                    result_cols.append(f"{col}_mean")
                elif func == "min":
                    agg_exprs.append(
                        f"MIN(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_min"
                    )
                    result_cols.append(f"{col}_min")
                elif func == "max":
                    agg_exprs.append(
                        f"MAX(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_max"
                    )
                    result_cols.append(f"{col}_max")
                elif func == "count":
                    agg_exprs.append(
                        f"COUNT(json_extract(data, '$.{col}')) as {col}_count"
                    )
                    result_cols.append(f"{col}_count")
                elif func == "std":
                    # SQLite doesn't have STDDEV built-in, calculate manually
                    agg_exprs.append(
                        f"SQRT(AVG(CAST(json_extract(data, '$.{col}') AS REAL) * "
                        f"CAST(json_extract(data, '$.{col}') AS REAL)) - "
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL)) * "
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL))) as {col}_std"
                    )
                    result_cols.append(f"{col}_std")

        # build group by query
        group_by = ", ".join([f"json_extract(data, '$.{col}')" for col in groupby_cols])
        select_cols = ", ".join(
            [f"json_extract(data, '$.{col}') as {col}" for col in groupby_cols]
        )

        cursor = self.execute_sql(
            f"""
            SELECT {select_cols}, {', '.join(agg_exprs)}
            FROM dataset_records
            WHERE dataset_id = ?
            GROUP BY {group_by}
        """,
            (self.dataset_id,),
        )

        # create result dataset
        records = []
        for row in cursor:
            records.append(dict(row))

        return SQLiteDataset.from_records(records, database_config=self.database_config)

    def applymap(self, func, batch_size=1000, na_action=None, **kwargs):
        """
        Apply a function to every element of the dataset.
        Batch processing.
        """
        new_dataset_id = str(uuid.uuid4())

        with self.database_config.get_connection() as conn:
            # process in batches
            offset = 0
            while True:
                cursor = conn.execute(
                    """
                    SELECT row_num, data FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                    LIMIT ? OFFSET ?
                """,
                    (self.dataset_id, batch_size, offset),
                )

                batch = cursor.fetchall()
                if not batch:
                    break

                transformed_records = []
                for row in batch:
                    row_num = row["row_num"]
                    data = json.loads(row["data"])

                    transformed_data = {}
                    for key, value in data.items():
                        if na_action == "ignore" and (
                            value is None or (isinstance(value, float) and isnan(value))
                        ):
                            transformed_data[key] = value
                        else:
                            try:
                                transformed_data[key] = func(value)
                            except Exception:
                                transformed_data[key] = value

                    transformed_records.append(
                        (new_dataset_id, row_num, json.dumps(transformed_data))
                    )

                conn.executemany(
                    """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES (?, ?, ?)
                """,
                    transformed_records,
                )

                offset += batch_size

            conn.commit()

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self._columns,
            length=self._length,
        )

    def to_records(self, index=True, column_dtypes=None, index_dtypes=None):
        """Convert SQLiteDataset to a numpy structured array."""

        # first get all records
        records = []

        with self.database_config.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT row_num, data FROM dataset_records
                WHERE dataset_id = ?
                ORDER BY row_num
            """,
                (self.dataset_id,),
            )

            for row in cursor.fetchall():
                row_num = row["row_num"]
                data = json.loads(row["data"])

                if index:
                    # include the row number
                    record_tuple = (row_num,) + tuple(
                        data.get(col) for col in self._columns
                    )
                else:
                    # just the data values
                    record_tuple = tuple(data.get(col) for col in self._columns)

                records.append(record_tuple)

        # create dtype for structured array
        if index:
            dtype_list = [("index", index_dtypes or "i8")]  # default to int64 for index
        else:
            dtype_list = []

        # add column dtypes
        for col in self._columns:
            if column_dtypes and col in column_dtypes:
                dtype_list.append((col, column_dtypes[col]))
            else:
                # infer dtype from first non-null value
                dtype = "O"  # default to object
                for record in records:
                    idx = len(dtype_list) if index else len(dtype_list)
                    val = record[idx] if len(record) > idx else None
                    if val is not None:
                        if isinstance(val, (int, np.integer)):
                            dtype = "i8"
                        elif isinstance(val, (float, np.floating)):
                            dtype = "f8"
                        elif isinstance(val, bool):
                            dtype = "bool"
                        else:
                            dtype = "O"  # object for strings and others
                        break
                dtype_list.append((col, dtype))

        # create structured array
        if records:
            return np.recarray(records, dtype=dtype_list)
        else:
            return np.recarray([], dtype=dtype_list)

    def unique(self, column: Optional[str] = None):
        """Return unique values of a column."""
        if column is None:
            if len(self.columns) == 1:
                column = self.columns[0]
            else:
                raise ValueError(
                    "Must specify column for DataFrame with multiple columns"
                )

        json_extract = self._get_json_extract_expr(column)
        cursor = self.execute_sql(
            f"""
            SELECT DISTINCT {json_extract} as value
            FROM dataset_records
            WHERE dataset_id = ?
              AND {json_extract} IS NOT NULL
            ORDER BY value
        """,
            (self.dataset_id,),
        )

        return [row["value"] for row in self.fetch_all(cursor)]

    def squeeze(self, axis=None):
        """Squeeze 1 dimensional axis objects into scalars."""
        if len(self.columns) == 1 and len(self) == 1:
            # Single value
            return self._get_column(self.columns[0])[0]
        elif len(self.columns) == 1:
            # Single column, return as list
            return self._get_column(self.columns[0])
        return self  # Can't squeeze

    def duplicated(self, subset=None, keep="first"):
        """Return boolean Series denoting duplicate rows."""
        if subset is None:
            subset = self.columns
        elif isinstance(subset, str):
            subset = [subset]

        # Build grouping expression
        group_cols = ", ".join([self._get_json_extract_expr(col) for col in subset])

        # Use window function to identify duplicates
        cursor = self.execute_sql(
            f"""
            WITH dup_counts AS (
                SELECT 
                    record_id,
                    row_num,
                    ROW_NUMBER() OVER (PARTITION BY {group_cols} ORDER BY row_num) as rn,
                    COUNT(*) OVER (PARTITION BY {group_cols}) as cnt
                FROM dataset_records
                WHERE dataset_id = ?
            )
            SELECT 
                row_num,
                CASE 
                    WHEN cnt = 1 THEN 0
                    WHEN keep = 'first' AND rn = 1 THEN 0
                    WHEN keep = 'last' AND rn = cnt THEN 0
                    WHEN keep = false THEN 1
                    ELSE 1
                END as is_dup
            FROM dup_counts
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        return [bool(row["is_dup"]) for row in self.fetch_all(cursor)]

    def isna(self):
        """Detect missing values."""
        new_dataset_id = str(uuid.uuid4())

        # Create dataset with boolean values for null checks
        cursor = self.execute_sql(
            f"""
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])
            null_checks = {}
            for col in self.columns:
                null_checks[col] = data.get(col) is None
            records.append(
                (new_dataset_id, row["row_num"], self._serialise_json(null_checks))
            )

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def notna(self):
        """Detect non-missing values."""
        new_dataset_id = str(uuid.uuid4())

        # Create dataset with boolean values for non-null checks
        cursor = self.execute_sql(
            f"""
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])
            null_checks = {}
            for col in self.columns:
                null_checks[col] = data.get(col) is not None
            records.append(
                (new_dataset_id, row["row_num"], self._serialise_json(null_checks))
            )

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def head(self, n=5):
        """Return the first n rows."""
        new_dataset_id = str(uuid.uuid4())

        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, row_num, data
            FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
            LIMIT ?
        """,
            (new_dataset_id, self.dataset_id, n),
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def tail(self, n=5):
        """Return the last n rows."""
        new_dataset_id = str(uuid.uuid4())

        # SQLite doesn't have LIMIT with OFFSET from end, so we need a subquery
        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, 
                   ROW_NUMBER() OVER (ORDER BY row_num) - 1 as row_num,
                   data
            FROM (
                SELECT row_num, data
                FROM dataset_records
                WHERE dataset_id = ?
                ORDER BY row_num DESC
                LIMIT ?
            ) t
            ORDER BY row_num
        """,
            (new_dataset_id, self.dataset_id, n),
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def nunique(self, axis=0, dropna=True):
        """Count distinct observations."""
        if axis == 0:  # Column-wise
            result = {}
            for col in self.columns:
                if dropna:
                    cursor = self.execute_sql(
                        f"""
                        SELECT COUNT(DISTINCT json_extract(data, '$.{col}')) as cnt
                        FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, '$.{col}') IS NOT NULL
                    """,
                        (self.dataset_id,),
                    )
                else:
                    cursor = self.execute_sql(
                        f"""
                        SELECT COUNT(DISTINCT json_extract(data, '$.{col}')) as cnt
                        FROM dataset_records
                        WHERE dataset_id = ?
                    """,
                        (self.dataset_id,),
                    )
                result[col] = cursor.fetchone()["cnt"]
            return result
        else:  # Row-wise - count unique values per row
            results = []
            for _, row_data in self.iterrows():
                unique_vals = set(row_data.values())
                if dropna and None in unique_vals:
                    unique_vals.remove(None)
                results.append(len(unique_vals))
            return results

    def agg(self, func, axis=0, *args, **kwargs):
        """Aggregate using one or more operations."""
        if isinstance(func, str):
            # Single function for all columns
            if func in ["sum", "mean", "min", "max", "count", "std"]:
                func_dict = {col: func for col in self.columns}
                return self._execute_aggregation([], func_dict)
        elif isinstance(func, dict):
            # Different functions for different columns
            return self._execute_aggregation([], func)
        else:
            # Unsupported
            raise NotImplementedError(f"Aggregation with {type(func)} not implemented")

    def select_dtypes(self, include=None, exclude=None):
        """Return a subset of columns based on inferred dtypes."""
        # For SQLite, we need to infer types from actual data
        selected_columns = []

        for col in self.columns:
            # Sample some values to determine type
            cursor = self.execute_sql(
                f"""
                SELECT json_extract(data, '$.{col}') as value
                FROM dataset_records
                WHERE dataset_id = ?
                  AND json_extract(data, '$.{col}') IS NOT NULL
                LIMIT 10
            """,
                (self.dataset_id,),
            )

            values = [row["value"] for row in self.fetch_all(cursor)]
            if not values:
                continue

            # Infer type from values
            is_numeric = all(
                isinstance(v, (int, float))
                or (
                    isinstance(v, str) and v.replace(".", "").replace("-", "").isdigit()
                )
                for v in values
            )

            if include:
                include_list = include if isinstance(include, list) else [include]
                if is_numeric and any(
                    t in ["number", "numeric", float, int] for t in include_list
                ):
                    selected_columns.append(col)
                elif not is_numeric and any(
                    t in ["object", str, "string"] for t in include_list
                ):
                    selected_columns.append(col)
            elif exclude:
                exclude_list = exclude if isinstance(exclude, list) else [exclude]
                if is_numeric and not any(
                    t in ["number", "numeric", float, int] for t in exclude_list
                ):
                    selected_columns.append(col)
                elif not is_numeric and not any(
                    t in ["object", str, "string"] for t in exclude_list
                ):
                    selected_columns.append(col)
            else:
                selected_columns.append(col)

        return self._get_columns(selected_columns)

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        """Return cumulative sum."""
        new_dataset_id = str(uuid.uuid4())

        if axis == 0 or axis is None:  # Column-wise cumsum
            # Build cumulative sum expressions for each numeric column
            cumsum_exprs = []
            for col in self.columns:
                if skipna:
                    cumsum_exprs.append(
                        f"""
                        SUM(CAST(json_extract(data, '$.{col}') AS REAL)) 
                        OVER (ORDER BY row_num ROWS UNBOUNDED PRECEDING) as {col}
                    """
                    )
                else:
                    # Non-skipna version would need more complex logic
                    cumsum_exprs.append(
                        f"""
                        SUM(CAST(json_extract(data, '$.{col}') AS REAL)) 
                        OVER (ORDER BY row_num ROWS UNBOUNDED PRECEDING) as {col}
                    """
                    )

            # Build the new records with cumulative sums
            cursor = self.execute_sql(
                f"""
                SELECT row_num, {', '.join(cumsum_exprs)}
                FROM dataset_records
                WHERE dataset_id = ?
                ORDER BY row_num
            """,
                (self.dataset_id,),
            )

            records = []
            for row in self.fetch_all(cursor):
                data = {col: row[col] for col in self.columns}
                records.append(
                    (new_dataset_id, row["row_num"], self._serialise_json(data))
                )

            self.execute_many(
                """
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES (?, ?, ?)
            """,
                records,
            )

            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=self.columns,
            )
        else:
            raise NotImplementedError("Row-wise cumsum not implemented")

    def to_frame(self, name=None):
        """Convert Series to DataFrame - SQLite datasets are always frame-like."""
        if len(self.columns) == 1:
            # Already single column, just rename if needed
            if name:
                return self.rename(columns={self.columns[0]: name})
        return self

    def describe(self, percentiles=None, include=None, exclude=None):
        """Generate descriptive statistics."""
        # Get numeric columns
        numeric_cols = []
        for col in self.columns:
            cursor = self.execute_sql(
                f"""
                SELECT json_extract(data, '$.{col}') as value
                FROM dataset_records
                WHERE dataset_id = ?
                  AND json_extract(data, '$.{col}') IS NOT NULL
                LIMIT 1
            """,
                (self.dataset_id,),
            )

            row = self.fetch_one(cursor)
            if row and isinstance(row["value"], (int, float)):
                numeric_cols.append(col)

        if not numeric_cols:
            return SQLiteDataset(database_config=self.database_config)

        # Calculate statistics for each numeric column
        stats_data = []
        stat_names = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]

        for stat in stat_names:
            row_data = {"": stat}  # First column is the statistic name

            for col in numeric_cols:
                if stat == "count":
                    cursor = self.execute_sql(
                        f"""
                        SELECT COUNT(json_extract(data, '$.{col}')) as val
                        FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, '$.{col}') IS NOT NULL
                    """,
                        (self.dataset_id,),
                    )
                elif stat == "mean":
                    cursor = self.execute_sql(
                        f"""
                        SELECT AVG(CAST(json_extract(data, '$.{col}') AS REAL)) as val
                        FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, '$.{col}') IS NOT NULL
                    """,
                        (self.dataset_id,),
                    )
                elif stat == "min":
                    cursor = self.execute_sql(
                        f"""
                        SELECT MIN(CAST(json_extract(data, '$.{col}') AS REAL)) as val
                        FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, '$.{col}') IS NOT NULL
                    """,
                        (self.dataset_id,),
                    )
                elif stat == "max":
                    cursor = self.execute_sql(
                        f"""
                        SELECT MAX(CAST(json_extract(data, '$.{col}') AS REAL)) as val
                        FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, '$.{col}') IS NOT NULL
                    """,
                        (self.dataset_id,),
                    )
                else:
                    # TODO: for percentiles and std, we'd need more complex calculations
                    # Simplified version here
                    row_data[col] = 0.0
                    continue

                result = self.fetch_one(cursor)
                row_data[col] = result["val"] if result else 0.0

            stats_data.append(row_data)

        return SQLiteDataset.from_records(
            stats_data, database_config=self.database_config
        )

    def value_counts(
        self, normalise=False, sort=True, ascending=False, bins=None, dropna=True
    ):
        """Return a Series containing counts of unique values."""
        if len(self.columns) != 1:
            raise ValueError("value_counts() only works on single columns")

        col = self.columns[0]

        if dropna:
            where_clause = f"AND json_extract(data, '$.{col}') IS NOT NULL"
        else:
            where_clause = ""

        cursor = self.execute_sql(
            f"""
            SELECT json_extract(data, '$.{col}') as value, 
                   COUNT(*) as count
            FROM dataset_records
            WHERE dataset_id = ?
              {where_clause}
            GROUP BY value
            {'ORDER BY count ' + ('ASC' if ascending else 'DESC') if sort else ''}
        """,
            (self.dataset_id,),
        )

        results = self.fetch_all(cursor)

        if normalise:
            total = sum(r["count"] for r in results)
            return {r["value"]: r["count"] / total for r in results}
        else:
            return {r["value"]: r["count"] for r in results}

    def shift(self, periods=1, freq=None, axis=0, fill_value=None):
        """Shift index by desired number of periods."""
        new_dataset_id = str(uuid.uuid4())

        if axis == 0:  # Shift rows
            # Use LAG/LEAD window functions
            if periods > 0:
                # Shift forward (LAG)
                cursor = self.execute_sql(
                    f"""
                    SELECT 
                        row_num,
                        LAG(data, ?) OVER (ORDER BY row_num) as shifted_data
                    FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                """,
                    (periods, self.dataset_id),
                )
            else:
                # Shift backward (LEAD)
                cursor = self.execute_sql(
                    f"""
                    SELECT 
                        row_num,
                        LEAD(data, ?) OVER (ORDER BY row_num) as shifted_data
                    FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                """,
                    (-periods, self.dataset_id),
                )

            records = []
            for row in self.fetch_all(cursor):
                if row["shifted_data"] is None:
                    # Fill with fill_value
                    if fill_value is not None:
                        data = {col: fill_value for col in self.columns}
                    else:
                        data = {col: None for col in self.columns}
                    records.append(
                        (new_dataset_id, row["row_num"], self._serialise_json(data))
                    )
                else:
                    records.append(
                        (new_dataset_id, row["row_num"], row["shifted_data"])
                    )

            self.execute_many(
                """
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES (?, ?, ?)
            """,
                records,
            )

            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=self.columns,
                length=self._length,
            )
        else:
            raise NotImplementedError("Column-wise shift not implemented")

    @property
    def shape(self) -> Tuple[int, int]:
        """Return a tuple representing the dimensionality of the dataset."""
        return (len(self), len(self.columns))

    @property
    def dtypes(self):
        """Return the dtypes in the dataset."""
        # Infer dtypes from actual data
        dtype_dict = {}
        for col in self.columns:
            # Sample first non-null value
            cursor = self.execute_sql(
                """
                SELECT json_extract(data, ?) as value
                FROM dataset_records
                WHERE dataset_id = ?
                  AND json_extract(data, ?) IS NOT NULL
                LIMIT 10
            """,
                (f"$.{col}", self.dataset_id, f"$.{col}"),
            )

            values = [row["value"] for row in self.fetch_all(cursor)]
            if not values:
                dtype_dict[col] = "object"
            elif all(isinstance(v, bool) for v in values):
                dtype_dict[col] = "bool"
            elif all(
                isinstance(v, (int, float))
                or (
                    isinstance(v, str) and v.replace(".", "").replace("-", "").isdigit()
                )
                for v in values
            ):
                dtype_dict[col] = "float64"
            else:
                dtype_dict[col] = "object"

        return dtype_dict

    @property
    def values(self) -> List[list]:
        """Return array representation (list of lists) of the data."""
        # Return as list of lists
        result = []
        for _, row_data in self.iterrows():
            result.append([row_data.get(col) for col in self.columns])
        return result

    @property
    def str(self):
        """Vectorised string functions for Series and Index."""

        class StringAccessor:
            def __init__(self, dataset, column=None):
                self.dataset = dataset
                self.column = column

            def __getitem__(self, key):
                return StringAccessor(self.dataset, key)

            def upper(self):
                if self.column is None:
                    raise ValueError("Must specify column for string operations")
                new_dataset_id = str(uuid.uuid4())

                cursor = self.dataset.execute_sql(
                    """
                    SELECT row_num, data FROM dataset_records
                    WHERE dataset_id = ?
                """,
                    (self.dataset.dataset_id,),
                )

                records = []
                for row in self.dataset.fetch_all(cursor):
                    data = json.loads(row["data"])
                    if self.column in data and isinstance(data[self.column], str):
                        data[self.column] = data[self.column].upper()
                    records.append((new_dataset_id, row["row_num"], json.dumps(data)))

                self.dataset.execute_many(
                    """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES (?, ?, ?)
                """,
                    records,
                )

                return SQLiteDataset(
                    dataset_id=new_dataset_id,
                    database_config=self.dataset.database_config,
                    columns=self.dataset.columns,
                )

            def lower(self):
                if self.column is None:
                    raise ValueError("Must specify column for string operations")
                new_dataset_id = str(uuid.uuid4())

                cursor = self.dataset.execute_sql(
                    """
                    SELECT row_num, data FROM dataset_records
                    WHERE dataset_id = ?
                """,
                    (self.dataset.dataset_id,),
                )

                records = []
                for row in self.dataset.fetch_all(cursor):
                    data = json.loads(row["data"])
                    if self.column in data and isinstance(data[self.column], str):
                        data[self.column] = data[self.column].lower()
                    records.append((new_dataset_id, row["row_num"], json.dumps(data)))

                self.dataset.execute_many(
                    """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES (?, ?, ?)
                """,
                    records,
                )

                return SQLiteDataset(
                    dataset_id=new_dataset_id,
                    database_config=self.dataset.database_config,
                    columns=self.dataset.columns,
                )

            def contains(self, pattern):
                if self.column is None:
                    raise ValueError("Must specify column for string operations")

                results = []
                cursor = self.dataset.execute_sql(
                    """
                    SELECT json_extract(data, ?) as value
                    FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                """,
                    (f"$.{self.column}", self.dataset.dataset_id),
                )

                for row in self.dataset.fetch_all(cursor):
                    val = row["value"]
                    results.append(pattern in str(val) if val is not None else False)

                return results

        return StringAccessor(self)

    @property
    def dt(self):
        """Accessor object for datetime-like properties."""

        class DatetimeAccessor:
            def __init__(self, dataset, column=None):
                self.dataset = dataset
                self.column = column

            def __getitem__(self, key):
                return DatetimeAccessor(self.dataset, key)

            def year(self):
                if self.column is None:
                    raise ValueError("Must specify column for datetime operations")

                results = []
                cursor = self.dataset.execute_sql(
                    """
                    SELECT json_extract(data, ?) as value
                    FROM dataset_records
                    WHERE dataset_id = ?
                    ORDER BY row_num
                """,
                    (f"$.{self.column}", self.dataset.dataset_id),
                )

                for row in self.dataset.fetch_all(cursor):
                    val = row["value"]
                    if val and isinstance(val, str) and len(val) >= 4:
                        try:
                            results.append(int(val[:4]))
                        except:
                            results.append(None)
                    else:
                        results.append(None)

                return results

        return DatetimeAccessor(self)

    def isin(self, values):
        """Check whether each element is contained in values."""
        new_dataset_id = str(uuid.uuid4())
        values_set = set(values)

        cursor = self.execute_sql(
            """
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])
            isin_data = {}
            for col in self.columns:
                isin_data[col] = data.get(col) in values_set
            records.append(
                (new_dataset_id, row["row_num"], self._serialise_json(isin_data))
            )

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def at(self, row_label, col_label):
        """Access a single value for a row/column label pair."""
        cursor = self.execute_sql(
            """
            SELECT json_extract(data, ?) as value
            FROM dataset_records
            WHERE dataset_id = ? AND row_num = ?
        """,
            (f"$.{col_label}", self.dataset_id, row_label),
        )

        result = self.fetch_one(cursor)
        return result["value"] if result else None

    def iloc(self, row_indexer, col_indexer=None):
        """Purely integer-location based indexing for selection by position."""
        if isinstance(row_indexer, int):
            # Single row
            if col_indexer is None:
                return self._get_row(row_indexer)
            elif isinstance(col_indexer, int):
                # Single cell
                col_name = self.columns[col_indexer]
                return self.at(row_indexer, col_name)
            else:
                # Multiple columns from single row
                row_data = self._get_row(row_indexer)
                if isinstance(col_indexer, slice):
                    cols = self.columns[col_indexer]
                else:
                    cols = [self.columns[i] for i in col_indexer]
                return {col: row_data.get(col) for col in cols}
        else:
            # Multiple rows
            if isinstance(row_indexer, slice):
                start = row_indexer.start or 0
                stop = row_indexer.stop or len(self)
                step = row_indexer.step or 1
                rows = list(range(start, stop, step))
            else:
                rows = list(row_indexer)

            if col_indexer is None:
                # All columns
                return self._get_rows_by_indices(rows)
            else:
                # Specific columns
                if isinstance(col_indexer, slice):
                    cols = self.columns[col_indexer]
                elif isinstance(col_indexer, int):
                    cols = [self.columns[col_indexer]]
                else:
                    cols = [self.columns[i] for i in col_indexer]

                dataset = self._get_rows_by_indices(rows)
                return dataset._get_columns(cols)

    def _get_rows_by_indices(self, indices):
        """Get multiple rows by their indices."""
        new_dataset_id = str(uuid.uuid4())

        placeholders = ", ".join(["?" for _ in indices])
        self.execute_sql(
            f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, 
                   ROW_NUMBER() OVER (ORDER BY row_num) - 1,
                   data
            FROM dataset_records
            WHERE dataset_id = ? AND row_num IN ({placeholders})
            ORDER BY row_num
        """,
            (new_dataset_id, self.dataset_id, *indices),
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def map(self, mapper, na_action=None):
        """Map values using an input mapping or function."""
        if len(self.columns) != 1:
            raise ValueError("map() only works on single columns")

        col = self.columns[0]
        new_dataset_id = str(uuid.uuid4())

        cursor = self.execute_sql(
            """
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])
            val = data.get(col)

            if na_action == "ignore" and val is None:
                new_val = None
            elif callable(mapper):
                new_val = mapper(val)
            elif isinstance(mapper, dict):
                new_val = mapper.get(val, val)
            else:
                new_val = val

            data[col] = new_val
            records.append((new_dataset_id, row["row_num"], self._serialise_json(data)))

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def to_parquet(self, path=None, **kwargs):
        """Write dataset to parquet format."""
        # Convert to pandas first, then to parquet
        # TODO: implement parquet sqlite conversion without pandas
        import pandas as pd

        # Get all data
        data = []
        for _, row_data in self.iterrows():
            data.append(row_data)

        df = pd.DataFrame(data)  # i know i know

        if path:
            df.to_parquet(path, **kwargs)
        else:
            return df.to_parquet(**kwargs)

    def assign(self, **kwargs):
        """Assign new columns to dataset."""
        new_dataset_id = str(uuid.uuid4())

        # First copy existing data
        self.execute_sql(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, row_num, data
            FROM dataset_records
            WHERE dataset_id = ?
        """,
            (new_dataset_id, self.dataset_id),
        )

        # Now add new columns
        new_columns = list(self.columns)
        for col_name, col_values in kwargs.items():
            if callable(col_values):
                # It's a function, apply it
                values_list = []
                for _, row_data in self.iterrows():
                    values_list.append(col_values(row_data))
            elif hasattr(col_values, "__iter__") and not isinstance(col_values, str):
                values_list = list(col_values)
            else:
                # Single value for all rows
                values_list = [col_values] * len(self)

            # Update each row
            for idx, value in enumerate(values_list):
                self.execute_sql(
                    """
                    UPDATE dataset_records
                    SET data = json_set(data, ?, json(?))
                    WHERE dataset_id = ? AND row_num = ?
                """,
                    (f"$.{col_name}", json.dumps(value), new_dataset_id, idx),
                )

            if col_name not in new_columns:
                new_columns.append(col_name)

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=new_columns,
        )

    def _comparison_op(self, other, op):
        """Generic comparison operation."""
        new_dataset_id = str(uuid.uuid4())

        cursor = self.execute_sql(
            """
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])
            result_data = {}

            for col in self.columns:
                val = data.get(col)
                if isinstance(other, dict):
                    other_val = other.get(col, other)
                else:
                    other_val = other

                if op == "eq":
                    result_data[col] = val == other_val
                elif op == "ne":
                    result_data[col] = val != other_val
                elif op == "lt":
                    try:
                        result_data[col] = val < other_val
                    except:
                        result_data[col] = False
                elif op == "le":
                    try:
                        result_data[col] = val <= other_val
                    except:
                        result_data[col] = False
                elif op == "gt":
                    try:
                        result_data[col] = val > other_val
                    except:
                        result_data[col] = False
                elif op == "ge":
                    try:
                        result_data[col] = val >= other_val
                    except:
                        result_data[col] = False

            records.append(
                (new_dataset_id, row["row_num"], self._serialise_json(result_data))
            )

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )

    def eq(self, other, axis="columns", level=None):
        """Get Equal to of dataframe and other, element-wise."""
        return self._comparison_op(other, "eq")

    def ne(self, other, axis="columns", level=None):
        """Get Not equal to of dataframe and other, element-wise."""
        return self._comparison_op(other, "ne")

    def lt(self, other, axis="columns", level=None):
        """Get Less than of dataframe and other, element-wise."""
        return self._comparison_op(other, "lt")

    def le(self, other, axis="columns", level=None):
        """Get Less than or equal to of dataframe and other, element-wise."""
        return self._comparison_op(other, "le")

    def gt(self, other, axis="columns", level=None):
        """Get Greater than of dataframe and other, element-wise."""
        return self._comparison_op(other, "gt")

    def ge(self, other, axis="columns", level=None):
        """Get Greater than or equal to of dataframe and other, element-wise."""
        return self._comparison_op(other, "ge")

    def any(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether any element is True."""
        if axis == 0:  # Column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM dataset_records
                        WHERE dataset_id = ?
                          AND json_extract(data, ?) = 1
                    )
                """,
                    (self.dataset_id, f"$.{col}"),
                )
                result[col] = bool(cursor.fetchone()[0])
            return result
        else:  # Row-wise
            results = []
            for _, row_data in self.iterrows():
                results.append(any(bool(v) for v in row_data.values() if v is not None))
            return results

    def all(self, axis=0, bool_only=None, skipna=True, level=None, **kwargs):
        """Return whether all elements are True."""
        if axis == 0:  # Column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT NOT EXISTS(
                        SELECT 1 FROM dataset_records
                        WHERE dataset_id = ?
                          AND (json_extract(data, ?) = 0 
                               OR json_extract(data, ?) IS NULL)
                    )
                """,
                    (self.dataset_id, f"$.{col}", f"$.{col}"),
                )
                result[col] = bool(cursor.fetchone()[0])
            return result
        else:  # Row-wise
            results = []
            for _, row_data in self.iterrows():
                if skipna:
                    values = [v for v in row_data.values() if v is not None]
                    results.append(all(bool(v) for v in values) if values else True)
                else:
                    results.append(all(bool(v) for v in row_data.values()))
            return results

    def sum(
        self,
        axis=None,
        skipna=True,
        level=None,
        numeric_only=None,
        min_count=0,
        **kwargs,
    ):
        """Return the sum of the values."""
        if axis == 0 or axis is None:  # Column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT SUM(CAST(json_extract(data, ?) AS REAL)) as sum_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """,
                    (f"$.{col}", self.dataset_id, f"$.{col}"),
                )
                result[col] = cursor.fetchone()["sum_val"] or 0
            return result
        else:  # Row-wise
            results = []
            for _, row_data in self.iterrows():
                numeric_vals = [
                    v for v in row_data.values() if isinstance(v, (int, float))
                ]
                results.append(sum(numeric_vals) if numeric_vals else 0)
            return results

    def mean(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the mean of the values."""
        if axis == 0 or axis is None:  # Column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT AVG(CAST(json_extract(data, ?) AS REAL)) as avg_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """,
                    (f"$.{col}", self.dataset_id, f"$.{col}"),
                )
                result[col] = cursor.fetchone()["avg_val"]
            return result
        else:  # Row-wise
            results = []
            for _, row_data in self.iterrows():
                numeric_vals = [
                    v for v in row_data.values() if isinstance(v, (int, float))
                ]
                results.append(
                    sum(numeric_vals) / len(numeric_vals) if numeric_vals else None
                )
            return results

    def std(
        self, axis=None, skipna=True, level=None, ddof=1, numeric_only=None, **kwargs
    ):
        """Return sample standard deviation."""
        # SQLite doesn't have built-in STDDEV, so we calculate manually
        if axis == 0 or axis is None:  # Column-wise
            result = {}
            for col in self.columns:
                # Get values
                cursor = self.execute_sql(
                    """
                    SELECT json_extract(data, ?) as value
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """,
                    (f"$.{col}", self.dataset_id, f"$.{col}"),
                )

                values = [
                    float(row["value"])
                    for row in self.fetch_all(cursor)
                    if row["value"] is not None
                ]

                if len(values) > ddof:
                    mean_val = sum(values) / len(values)
                    variance = sum((x - mean_val) ** 2 for x in values) / (
                        len(values) - ddof
                    )
                    result[col] = variance**0.5
                else:
                    result[col] = None
            return result
        else:
            # Row-wise std
            results = []
            for _, row_data in self.iterrows():
                numeric_vals = [
                    v for v in row_data.values() if isinstance(v, (int, float))
                ]
                if len(numeric_vals) > ddof:
                    mean_val = sum(numeric_vals) / len(numeric_vals)
                    variance = sum((x - mean_val) ** 2 for x in numeric_vals) / (
                        len(numeric_vals) - ddof
                    )
                    results.append(variance**0.5)
                else:
                    results.append(None)
            return results

    def max(self, axis=None, skipna=True, level=None, numeric_only=None, **kwargs):
        """Return the maximum of the values."""
        # Already implemented in parent min method, use similar approach
        if axis == 0 or axis is None:  # Column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql(
                    """
                    SELECT MAX(CAST(json_extract(data, ?) AS REAL)) as max_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """,
                    (f"$.{col}", self.dataset_id, f"$.{col}"),
                )
                result[col] = cursor.fetchone()["max_val"]
            return result
        else:  # Row-wise
            results = []
            for _, row_data in self.iterrows():
                numeric_vals = [
                    v for v in row_data.values() if isinstance(v, (int, float))
                ]
                results.append(max(numeric_vals) if numeric_vals else None)
            return results

    def round(self, decimals=0, *args, **kwargs):
        """Round to a variable number of decimal places."""
        new_dataset_id = str(uuid.uuid4())

        cursor = self.execute_sql(
            """
            SELECT row_num, data FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """,
            (self.dataset_id,),
        )

        records = []
        for row in self.fetch_all(cursor):
            data = self._parse_json(row["data"])

            for col in self.columns:
                val = data.get(col)
                if isinstance(val, (int, float)):
                    if isinstance(decimals, dict):
                        dec = decimals.get(col, 0)
                    else:
                        dec = decimals
                    data[col] = round(val, dec)

            records.append((new_dataset_id, row["row_num"], self._serialise_json(data)))

        self.execute_many(
            """
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """,
            records,
        )

        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=self.columns,
        )
