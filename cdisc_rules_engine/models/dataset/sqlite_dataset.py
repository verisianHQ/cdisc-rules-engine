import json
import uuid

from typing import List, Dict, Any, Union

from cdisc_rules_engine.models.dataset.sql_dataset_base import SQLDatasetBase
from cdisc_rules_engine.config.databases.sqlite_database_config import SQLiteDatabaseConfig


class SQLiteDataset(SQLDatasetBase):
    """SQLite-backed dataset implementation."""

    def __init__(self,
                 dataset_id: str = None,
                 database_config: SQLiteDatabaseConfig = None,
                 columns=None,
                 table_name=None,
                 length=None):

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
    
    def _create_dataset_entry(self):
        """Register dataset in metadata table."""
        self.execute_sql(
            """
                INSERT OR IGNORE INTO datasets (dataset_id, dataset_name) 
                VALUES (?, ?)
            """, (self.dataset_id, self._table_name))
    
    def _insert_records(self, records: List[dict]):
        """Bulk insert records into database."""
        if not records:
            return
        
        values = [(self.dataset_id, idx, json.dumps(record)) 
                 for idx, record in enumerate(records)]
        
        self.execute_many("""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """, values)
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
        return '?'
    
    def _parse_json(self, json_data: Any) -> dict:
        """Parse JSON string to dict."""
        return json.loads(json_data) if isinstance(json_data, str) else json_data
    
    def _serialise_json(self, data: dict) -> str:
        """Serialise dict to JSON for SQLite."""
        return json.dumps(data)
    
    def _set_column_value(self, column: str, value: Any, row_idx: int):
        """Set a single cell value."""
        self.execute_sql("""
            UPDATE dataset_records
            SET data = json_set(data, ?, json(?))
            WHERE dataset_id = ? AND row_num = ?
        """, (f'$.{column}', json.dumps(value), self.dataset_id, row_idx))
    
    def _set_column_value_all(self, column: str, value: Any):
        """Set all rows in a column to the same value."""
        self.execute_sql("""
            UPDATE dataset_records
            SET data = json_set(data, ?, json(?))
            WHERE dataset_id = ?
        """, (f'$.{column}', json.dumps(value), self.dataset_id))
    
    def _get_columns(self, column_names: List[str]) -> 'SQLiteDataset':
        """Get multiple columns as new dataset."""
        new_dataset_id = str(uuid.uuid4())
        
        # Build JSON object with selected columns
        json_build = self._get_json_build_object_expr(column_names)
        
        self.execute_sql(f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT ?, row_num, {json_build}
            FROM dataset_records
            WHERE dataset_id = ?
            ORDER BY row_num
        """, (new_dataset_id, self.dataset_id))
        
        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=column_names
        )
    
    def rename(self, index=None, columns=None, inplace=True):
        """Rename columns."""
        if columns:
            with self.database_config.get_connection() as conn:
                for old_name, new_name in columns.items():
                    # Update each record's JSON
                    cursor = conn.execute("""
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """, (self.dataset_id,))
                    
                    for row in cursor.fetchall():
                        data = json.loads(row['data'])
                        if old_name in data:
                            data[new_name] = data.pop(old_name)
                            conn.execute("""
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """, (json.dumps(data), row['record_id']))
                
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
            cursor = conn.execute("""
                SELECT metadata FROM datasets WHERE dataset_id = ?
            """, (self.dataset_id,))
            
            row = cursor.fetchone()
            metadata = json.loads(row['metadata'] or '{}') if row else {}
            metadata['index_columns'] = keys
            
            conn.execute("""
                UPDATE datasets
                SET metadata = ?
                WHERE dataset_id = ?
            """, (json.dumps(metadata), self.dataset_id))
            conn.commit()
        
        return self
    
    def _bulk_insert_melt_records(self, records: List[tuple]):
        """Bulk insert records for melt operation."""
        self.execute_many("""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """, records)
    
    def _bulk_insert_error_rows(self, rows: List[tuple]):
        """Bulk insert error rows."""
        self.execute_many("""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """, rows)
    
    def _bulk_insert_where_rows(self, rows: List[tuple]):
        """Bulk insert filtered rows."""
        self.execute_many("""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            VALUES (?, ?, ?)
        """, rows)

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
                    if errors == 'raise' and col not in self._columns:
                        raise KeyError(f"Column '{col}' not found")
                    
                    # Update each record's JSON
                    cursor = conn.execute("""
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """, (self.dataset_id,))
                    
                    for row in cursor.fetchall():
                        data = json.loads(row['data'])
                        if col in data:
                            del data[col]
                            conn.execute("""
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """, (json.dumps(data), row['record_id']))
                conn.commit()
            
            # update columns
            self._columns = [c for c in self._columns if c not in cols_to_drop]
            self._register_columns(self._columns)
        
        else:  # drop rows
            if isinstance(labels, int):
                labels = [labels]
            
            with self.database_config.get_connection() as conn:
                for label in labels:
                    conn.execute("""
                        DELETE FROM dataset_records
                        WHERE dataset_id = ? AND row_num = ?
                    """, (self.dataset_id, label))
                
                # reindex remaining rows
                conn.execute("""
                    UPDATE dataset_records
                    SET row_num = (
                        SELECT COUNT(*) 
                        FROM dataset_records dr2 
                        WHERE dr2.dataset_id = dataset_records.dataset_id 
                          AND dr2.row_num < dataset_records.row_num
                    )
                    WHERE dataset_id = ?
                """, (self.dataset_id,))
                conn.commit()
            
            self._length = None  # reset cached length
        
    def concat(self, other: Union['SQLiteDataset', List['SQLiteDataset']], 
               axis=0, **kwargs):
        """Concatenate datasets."""
        if axis == 0:  # vertical concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())
            
            with self.database_config.get_connection() as conn:
                # copy self first
                conn.execute("""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT ?, row_num, data
                    FROM dataset_records
                    WHERE dataset_id = ?
                """, (new_dataset_id, self.dataset_id))
                
                # append others
                offset = len(self)
                for ds in datasets:
                    conn.execute("""
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT ?, row_num + ?, data
                        FROM dataset_records
                        WHERE dataset_id = ?
                    """, (new_dataset_id, offset, ds.dataset_id))
                    offset += len(ds)
                conn.commit()
            
            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=self._columns
            )
        else:  # horizontal concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())
            
            # Build a query that merges JSON objects
            with self.database_config.get_connection() as conn:
                # First, create temp table with all data
                temp_table = f"temp_concat_{new_dataset_id.replace('-', '_')}"
                conn.execute(f"""
                    CREATE TEMP TABLE {temp_table} AS
                    SELECT row_num, data as data0 FROM dataset_records WHERE dataset_id = ?
                """, (self.dataset_id,))
                
                # Add columns from other datasets
                for i, ds in enumerate(datasets):
                    conn.execute(f"""
                        ALTER TABLE {temp_table}
                        ADD COLUMN data{i+1} TEXT
                    """)
                    
                    conn.execute(f"""
                        UPDATE {temp_table}
                        SET data{i+1} = (
                            SELECT data FROM dataset_records 
                            WHERE dataset_id = ? AND row_num = {temp_table}.row_num
                        )
                    """, (ds.dataset_id,))
                
                # Merge JSON objects
                merge_expr = "json(data0)"
                for i in range(len(datasets)):
                    merge_expr = f"json_patch({merge_expr}, json(data{i+1}))"
                
                conn.execute(f"""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT ?, row_num, {merge_expr}
                    FROM {temp_table}
                """, (new_dataset_id,))
                
                conn.execute(f"DROP TABLE {temp_table}")
                conn.commit()
            
            # combine columns
            all_columns = list(self._columns)
            for ds in datasets:
                all_columns.extend([c for c in ds.columns if c not in all_columns])
            
            return SQLiteDataset(
                dataset_id=new_dataset_id,
                database_config=self.database_config,
                columns=all_columns
            )
    
    def merge(self, other: type['SQLDatasetBase'], on=None, how='inner', **kwargs):
        """Merge datasets using sql join."""
        join_type_map = {
            'inner': 'INNER JOIN',
            'left': 'LEFT JOIN',
            'right': 'RIGHT JOIN',
            'outer': 'LEFT JOIN',  # SQLite doesn't have FULL OUTER, simulate with UNION
            'cross': 'CROSS JOIN'
        }
        
        join_type = join_type_map.get(how, 'INNER JOIN')
        new_dataset_id = str(uuid.uuid4())
        
        if on:
            if isinstance(on, str):
                on = [on]
            join_conditions = ' AND '.join([
                f"json_extract(a.data, '$.{col}') = json_extract(b.data, '$.{col}')" 
                for col in on
            ])
        else:
            join_conditions = '1=1'
        
        with self.database_config.get_connection() as conn:
            if how == 'outer':
                # Simulate FULL OUTER JOIN with UNION
                conn.execute(f"""
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
                """, (new_dataset_id, other.dataset_id, self.dataset_id, 
                     self.dataset_id, other.dataset_id))
            else:
                conn.execute(f"""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT 
                        ?,
                        ROW_NUMBER() OVER (ORDER BY a.row_num, b.row_num) - 1,
                        json_patch(json(a.data), json(b.data))
                    FROM dataset_records a
                    {join_type} dataset_records b 
                        ON {join_conditions} AND b.dataset_id = ?
                    WHERE a.dataset_id = ?
                """, (new_dataset_id, other.dataset_id, self.dataset_id))
            conn.commit()
        
        # combine columns
        merged_columns = list(self._columns)
        merged_columns.extend([c for c in other.columns if c not in merged_columns])
        
        return SQLiteDataset(
            dataset_id=new_dataset_id,
            database_config=self.database_config,
            columns=merged_columns
        )
    
    def _build_order_clause(self, columns: List[str], ascending: bool) -> str:
        """Build ORDER BY clause with SQLite type casting."""
        direction = 'ASC' if ascending else 'DESC'
        order_parts = []
        for col in columns:
            # Cast to REAL for numeric sorting, fallback to text
            order_parts.append(
                f"CAST(json_extract(data, '$.{col}') AS REAL) {direction}, "
                f"json_extract(data, '$.{col}') {direction}"
            )
        return ', '.join(order_parts)
    
    def is_column_sorted_within(self, group: Union[str, List[str]], column: str) -> bool:
        """Check if column is sorted within groups."""
        if isinstance(group, str):
            group = [group]
        
        # Build partition clause
        partition_cols = ', '.join([f"json_extract(data, '$.{g}')" for g in group])
        
        cursor = self.execute_sql(f"""
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
        """, (self.dataset_id,))
        
        return not cursor.fetchone()[0]
    
    def min(self, axis=0, skipna=True, **kwargs):
        """Get minimum values."""
        if axis == 0:  # column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql("""
                    SELECT MIN(CAST(json_extract(data, ?) AS REAL)) as min_val
                    FROM dataset_records
                    WHERE dataset_id = ?
                      AND json_extract(data, ?) IS NOT NULL
                """, (f'$.{col}', self.dataset_id, f'$.{col}'))
                result[col] = cursor.fetchone()['min_val']
            return result
        else:  # row-wise
            results = []
            for row_num, row_data in self.iterrows():
                numeric_vals = [v for v in row_data.values() 
                               if isinstance(v, (int, float))]
                results.append(min(numeric_vals) if numeric_vals else None)
            return results
    
    def reset_index(self, drop=False, **kwargs):
        """Reset row numbers to sequential."""
        with self.database_config.get_connection() as conn:
            if not drop:
                # save current row_num as index column
                cursor = conn.execute("""
                    SELECT record_id, row_num, data FROM dataset_records
                    WHERE dataset_id = ?
                """, (self.dataset_id,))
                
                for row in cursor.fetchall():
                    data = json.loads(row['data'])
                    data['index'] = row['row_num']
                    conn.execute("""
                        UPDATE dataset_records
                        SET data = ?
                        WHERE record_id = ?
                    """, (json.dumps(data), row['record_id']))
                
                if 'index' not in self._columns:
                    self._columns.insert(0, 'index')
                    self._register_columns(self._columns)
            
            # resequence row numbers
            conn.execute("""
                UPDATE dataset_records
                SET row_num = (
                    SELECT COUNT(*) 
                    FROM dataset_records dr2 
                    WHERE dr2.dataset_id = dataset_records.dataset_id 
                      AND dr2.row_num < dataset_records.row_num
                )
                WHERE dataset_id = ?
            """, (self.dataset_id,))
            conn.commit()
    
    def fillna(self, value=None, method=None, axis=None, 
               inplace=False, limit=None, downcast=None):
        """Fill null values."""
        dataset = self if inplace else self.copy()
        
        with self.database_config.get_connection() as conn:
            if value is not None:
                # fill with specific value
                for col in dataset.columns:
                    cursor = conn.execute("""
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                    """, (dataset.dataset_id,))
                    
                    for row in cursor.fetchall():
                        data = json.loads(row['data'])
                        if col not in data or data[col] is None:
                            data[col] = value
                            conn.execute("""
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """, (json.dumps(data), row['record_id']))
            
            elif method == 'ffill':
                # forward fill
                for col in dataset.columns:
                    last_valid = None
                    cursor = conn.execute("""
                        SELECT record_id, data FROM dataset_records
                        WHERE dataset_id = ?
                        ORDER BY row_num
                    """, (dataset.dataset_id,))
                    
                    for row in cursor.fetchall():
                        data = json.loads(row['data'])
                        if col in data and data[col] is not None:
                            last_valid = data[col]
                        elif last_valid is not None:
                            data[col] = last_valid
                            conn.execute("""
                                UPDATE dataset_records
                                SET data = ?
                                WHERE record_id = ?
                            """, (json.dumps(data), row['record_id']))
            
            conn.commit()
        
        return dataset
    
    def _execute_aggregation(self, groupby_cols: List[str], 
                            func_dict: Dict[str, Union[str, List[str]]]):
        """Execute aggregation query."""
        # build aggregation expressions
        agg_exprs = []
        result_cols = list(groupby_cols)
        
        for col, funcs in func_dict.items():
            if isinstance(funcs, str):
                funcs = [funcs]
            
            for func in funcs:
                if func == 'sum':
                    agg_exprs.append(
                        f"SUM(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_sum"
                    )
                    result_cols.append(f"{col}_sum")
                elif func == 'mean' or func == 'avg':
                    agg_exprs.append(
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_mean"
                    )
                    result_cols.append(f"{col}_mean")
                elif func == 'min':
                    agg_exprs.append(
                        f"MIN(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_min"
                    )
                    result_cols.append(f"{col}_min")
                elif func == 'max':
                    agg_exprs.append(
                        f"MAX(CAST(json_extract(data, '$.{col}') AS REAL)) as {col}_max"
                    )
                    result_cols.append(f"{col}_max")
                elif func == 'count':
                    agg_exprs.append(f"COUNT(json_extract(data, '$.{col}')) as {col}_count")
                    result_cols.append(f"{col}_count")
                elif func == 'std':
                    # SQLite doesn't have STDDEV built-in, calculate manually
                    agg_exprs.append(
                        f"SQRT(AVG(CAST(json_extract(data, '$.{col}') AS REAL) * "
                        f"CAST(json_extract(data, '$.{col}') AS REAL)) - "
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL)) * "
                        f"AVG(CAST(json_extract(data, '$.{col}') AS REAL))) as {col}_std"
                    )
                    result_cols.append(f"{col}_std")
        
        # build group by query
        group_by = ', '.join([f"json_extract(data, '$.{col}')" for col in groupby_cols])
        select_cols = ', '.join([f"json_extract(data, '$.{col}') as {col}" 
                                for col in groupby_cols])
        
        cursor = self.execute_sql(f"""
            SELECT {select_cols}, {', '.join(agg_exprs)}
            FROM dataset_records
            WHERE dataset_id = ?
            GROUP BY {group_by}
        """, (self.dataset_id,))
        
        # create result dataset
        records = []
        for row in cursor:
            records.append(dict(row))
        
        return SQLiteDataset.from_records(
            records,
            database_config=self.database_config
        )