import uuid

from psycopg2.extras import Json, execute_values
from typing import List, Dict, Any, Union

from cdisc_rules_engine.models.dataset.sql_dataset_base import SQLDatasetBase
from cdisc_rules_engine.config.databases.postgresql_database_config import PostgreSQLDatabaseConfig

class PostgreSQLDataset(SQLDatasetBase):
    """PostgreSQL-backed dataset implementation."""

    def __init__(self,
                 dataset_id: str = None,
                 database_config: PostgreSQLDatabaseConfig = None,
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

    # ========== PostgreSQL-specific methods ==========

    def execute_sql(self, sql_code: str, args: tuple) -> Any:
        """Execute sql code on cursor."""
        with self.database_config.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_code, args)
                return cur
        return None

    def execute_sql_values(self, sql_code: str, args: List[tuple]):
        """Execute value sql code on cursor."""
        with self.database_config.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql_code, args)

    def fetch_all(self, cursor=None) -> List[Any]:
        """Fetch all data."""
        if cursor:
            return cursor.fetchall()
        return []
    
    def fetch_one(self, cursor=None) -> Any:
        """Fetch one row."""
        if cursor:
            return cursor.fetchone()
        return None
    
    def _create_dataset_entry(self):
        """Register dataset in metadata table."""
        self.execute_sql(
            """
                INSERT INTO datasets (dataset_id, dataset_name) 
                VALUES (%s, %s)
                ON CONFLICT (dataset_name, dataset_type) DO NOTHING
            """, (self.dataset_id, self._table_name))
    
    def _insert_records(self, records: List[dict]):
        """Bulk insert records into database."""
        if not records:
            return

        values = [(self.dataset_id, idx, Json(record)) 
                    for idx, record in enumerate(records)]
        self.execute_sql_values("""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES %s
            """, values)
        self._length = len(records)
    
    def _get_json_extract_expr(self, column_name: str) -> str:
        """Get SQL expression for extracting JSON field value."""
        return f"data->>'{column_name}'"
    
    def _get_json_build_object_expr(self, columns: List[str]) -> str:
        """Get SQL expression for building JSON object."""
        pairs = []
        for col in columns:
            pairs.append(f"'{col}', data->>'{col}'")
        return f"json_build_object({', '.join(pairs)})::jsonb"
    
    def _get_json_set_expr(self, column_name: str, value_placeholder: str) -> str:
        """Get SQL expression for setting JSON field."""
        return f"jsonb_set(data, %s, {value_placeholder})"
    
    def _get_json_merge_expr(self, *json_exprs: str) -> str:
        """Get SQL expression for merging JSON objects."""
        return ' || '.join(json_exprs)
    
    def _get_placeholder(self) -> str:
        """PostgreSQL uses %s for placeholders."""
        return '%s'
    
    def _parse_json(self, json_data: Any) -> dict:
        """PostgreSQL returns dict directly from jsonb."""
        return json_data
    
    def _serialise_json(self, data: dict) -> Any:
        """Serialise dict to JSON for PostgreSQL."""
        return Json(data)
    
    def _set_column_value(self, column: str, value: Any, row_idx: int):
        """Set a single cell value."""
        self.execute_sql("""
            UPDATE dataset_records
            SET data = jsonb_set(data, %s, %s)
            WHERE dataset_id = %s AND row_num = %s
        """, ([column], Json(value), self.dataset_id, row_idx))
    
    def _set_column_value_all(self, column: str, value: Any):
        """Set all rows in a column to the same value."""
        self.execute_sql("""
            UPDATE dataset_records
            SET data = jsonb_set(data, %s, %s)
            WHERE dataset_id = %s
        """, ([column], Json(value), self.dataset_id))
    
    def _get_columns(self, column_names: List[str]) -> 'PostgreSQLDataset':
        """Get multiple columns as new dataset."""
        json_build = self._get_json_build_object_expr(column_names)
        new_dataset_id = str(uuid.uuid4())
        
        self.execute_sql(f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT %s, row_num, {json_build}
            FROM dataset_records
            WHERE dataset_id = %s
            ORDER BY row_num
        """, (new_dataset_id, self.dataset_id))
        
        return PostgreSQLDataset(
            dataset_id=new_dataset_id,
            connection_pool=self.database_config,
            columns=column_names
        )
    
    def rename(self, index=None, columns=None, inplace=True):
        """Rename columns."""
        if columns:
            for old_name, new_name in columns.items():
                self.execute_sql("""
                    UPDATE dataset_records
                    SET data = data - %s || jsonb_build_object(%s, data->%s)
                    WHERE dataset_id = %s
                """, (old_name, new_name, old_name, self.dataset_id))
            
            # update columns list
            self._columns = [columns.get(c, c) for c in self._columns]
            self._register_columns(self._columns)
    
    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """Drop rows or columns."""
        if axis == 1 or columns:  # drop columns
            cols_to_drop = columns or labels
            if isinstance(cols_to_drop, str):
                cols_to_drop = [cols_to_drop]
            
            # remove from data
            for col in cols_to_drop:
                if errors == 'raise' and col not in self._columns:
                    raise KeyError(f"Column '{col}' not found")
                
                self.execute_sql("""
                    UPDATE dataset_records
                    SET data = data - %s
                    WHERE dataset_id = %s
                """, (col, self.dataset_id))
            
            # update columns
            self._columns = [c for c in self._columns 
                            if c not in cols_to_drop]
            self._register_columns(self._columns)
        
        else:  # drop rows
            if isinstance(labels, int):
                labels = [labels]
            
            for label in labels:
                self.execute_sql("""
                    DELETE FROM dataset_records
                    WHERE dataset_id = %s AND row_num = %s
                """, (self.dataset_id, label))
            
            # reindex remaining rows
            self.execute_sql("""
                UPDATE dataset_records
                SET row_num = new_num - 1
                FROM (
                    SELECT record_id, 
                           ROW_NUMBER() OVER (ORDER BY row_num) as new_num
                    FROM dataset_records
                    WHERE dataset_id = %s
                ) as reindexed
                WHERE dataset_records.record_id = reindexed.record_id
            """, (self.dataset_id,))
            
            self._length = None  # reset cached length
    
    def _bulk_insert_melt_records(self, records: List[tuple]):
        """Bulk insert records for melt operation."""
        self.execute_sql_values("""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES %s
            """, records)
    
    def set_index(self, keys, **kwargs):
        """Set index columns (stored as metadata)."""
        if isinstance(keys, str):
            keys = [keys]
        
        self.execute_sql("""
            UPDATE datasets
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'), 
                '{index_columns}', 
                %s
            )
            WHERE dataset_id = %s
        """, (Json(keys), self.dataset_id))
        
        return self
    
    def _bulk_insert_error_rows(self, rows: List[tuple]):
        """Bulk insert error rows."""
        self.execute_sql_values("""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES %s
            """, rows)
    
    def _bulk_insert_where_rows(self, rows: List[tuple]):
        """Bulk insert filtered rows."""
        self.execute_sql_values("""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                VALUES %s
            """, rows)
    
    def concat(self, other: Union['PostgreSQLDataset', List['PostgreSQLDataset']], 
               axis=0, **kwargs):
        """Concatenate datasets."""
        if axis == 0:  # vertical concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())
        
            # copy self first
            self.execute_sql("""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                SELECT %s, row_num, data
                FROM dataset_records
                WHERE dataset_id = %s
            """, (new_dataset_id, self.dataset_id))
            
            # append others
            offset = len(self)
            for ds in datasets:
                self.execute_sql("""
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    SELECT %s, row_num + %s, data
                    FROM dataset_records
                    WHERE dataset_id = %s
                """, (new_dataset_id, offset, ds.dataset_id))
                offset += len(ds)
            
            return PostgreSQLDataset(
                dataset_id=new_dataset_id,
                connection_pool=self.database_config,
                columns=self._columns
            )
        else:  # horizontal concat
            datasets = [other] if not isinstance(other, list) else other
            new_dataset_id = str(uuid.uuid4())
            
            # merge columns from all datasets
            merge_sql = "a.data"
            for i, ds in enumerate(datasets):
                merge_sql += f" || b{i}.data"
            
            # build join conditions
            join_parts = []
            for i, ds in enumerate(datasets):
                join_parts.append(f"""
                    LEFT JOIN dataset_records b{i} 
                    ON a.row_num = b{i}.row_num 
                    AND b{i}.dataset_id = '{ds.dataset_id}'
                """)
            
            self.execute_sql(f"""
                INSERT INTO dataset_records (dataset_id, row_num, data)
                SELECT %s, a.row_num, {merge_sql}
                FROM dataset_records a
                {' '.join(join_parts)}
                WHERE a.dataset_id = %s
            """, (new_dataset_id, self.dataset_id))
            
            # combine columns
            all_columns = list(self._columns)
            for ds in datasets:
                all_columns.extend([c for c in ds.columns 
                                    if c not in all_columns])
            
            return PostgreSQLDataset(
                dataset_id=new_dataset_id,
                connection_pool=self.database_config,
                columns=all_columns
            )
    
    def merge(self, other: 'PostgreSQLDataset', on=None, how='inner', **kwargs):
        """Merge datasets using sql join."""
        join_type_map = {
            'inner': 'INNER JOIN',
            'left': 'LEFT JOIN',
            'right': 'RIGHT JOIN',
            'outer': 'FULL OUTER JOIN',
            'cross': 'CROSS JOIN'
        }
        
        join_type = join_type_map.get(how, 'INNER JOIN')
        new_dataset_id = str(uuid.uuid4())
        
        if on:
            if isinstance(on, str):
                on = [on]
            join_conditions = ' AND '.join([
                f"a.data->>'{col}' = b.data->>'{col}'" for col in on
            ])
        else:
            join_conditions = '1=1'
        
        self.execute_sql(f"""
            INSERT INTO dataset_records (dataset_id, row_num, data)
            SELECT 
                %s,
                ROW_NUMBER() OVER (ORDER BY a.row_num, b.row_num) - 1,
                a.data || b.data
            FROM dataset_records a
            {join_type} dataset_records b 
                ON {join_conditions}
            WHERE a.dataset_id = %s 
                AND b.dataset_id = %s
        """, (new_dataset_id, self.dataset_id, other.dataset_id))
        
        # combine columns
        merged_columns = list(self._columns)
        merged_columns.extend([c for c in other.columns 
                                if c not in merged_columns])
        
        return PostgreSQLDataset(
            dataset_id=new_dataset_id,
            connection_pool=self.database_config,
            columns=merged_columns
        )
    
    def _build_order_clause(self, columns: List[str], ascending: bool) -> str:
        """Build ORDER BY clause for sorting."""
        direction = 'ASC' if ascending else 'DESC'
        order_parts = []
        for col in columns:
            order_parts.append(f"(data->>'{col}') {direction}")
        return ', '.join(order_parts)
    
    def is_column_sorted_within(self, group: Union[str, List[str]], column: str) -> bool:
        """Check if column is sorted within groups."""
        if isinstance(group, str):
            group = [group]
        
        # use window function to check ordering
        group_cols = ', '.join([f"data->>'{g}'" for g in group])
        
        cursor = self.execute_sql(f"""
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT data->>{self._get_placeholder()} as col_val,
                           LAG(data->>{self._get_placeholder()}) OVER (
                               PARTITION BY {group_cols}
                               ORDER BY row_num
                           ) as prev_val
                    FROM dataset_records
                    WHERE dataset_id = {self._get_placeholder()}
                ) t
                WHERE prev_val IS NOT NULL 
                    AND prev_val > col_val
            )
        """, (column, column, self.dataset_id))
        
        return not self.fetch_one(cursor)[0]
    
    def min(self, axis=0, skipna=True, **kwargs):
        """Get minimum values."""
        if axis == 0:  # column-wise
            result = {}
            for col in self.columns:
                cursor = self.execute_sql("""
                    SELECT MIN((data->%s)::numeric)
                    FROM dataset_records
                    WHERE dataset_id = %s
                        AND data ? %s
                """, (col, self.dataset_id, col))
                result[col] = self.fetch_one(cursor)[0]
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
        if not drop:
            # save current row_num as index column
            self.execute_sql("""
                UPDATE dataset_records
                SET data = jsonb_set(data, '{index}', to_jsonb(row_num))
                WHERE dataset_id = %s
            """, (self.dataset_id,))
            
            if 'index' not in self._columns:
                self._columns.insert(0, 'index')
                self._register_columns(self._columns)
        
        # resequence row numbers
        self.execute_sql("""
            UPDATE dataset_records dr
            SET row_num = t.new_row_num - 1
            FROM (
                SELECT record_id,
                       ROW_NUMBER() OVER (ORDER BY row_num) as new_row_num
                FROM dataset_records
                WHERE dataset_id = %s
            ) t
            WHERE dr.record_id = t.record_id
        """, (self.dataset_id,))
    
    def fillna(self, value=None, method=None, axis=None, 
               inplace=False, limit=None, downcast=None):
        """Fill null values."""
        dataset = self if inplace else self.copy()
        
        if value is not None:
            # fill with specific value
            for col in dataset.columns:
                dataset.execute_sql("""
                    UPDATE dataset_records
                    SET data = jsonb_set(
                        data, 
                        %s,
                        COALESCE(data->%s, %s)
                    )
                    WHERE dataset_id = %s
                """, ([col], col, Json(value), dataset.dataset_id))
        
        elif method == 'ffill':
            # forward fill
            for col in dataset.columns:
                dataset.execute_sql(f"""
                    UPDATE dataset_records dr
                    SET data = jsonb_set(
                        data,
                        %s,
                        COALESCE(
                            data->%s,
                            (SELECT data->%s
                             FROM dataset_records dr2
                             WHERE dr2.dataset_id = dr.dataset_id
                               AND dr2.row_num < dr.row_num
                               AND dr2.data ? %s
                             ORDER BY dr2.row_num DESC
                             LIMIT 1)
                        )
                    )
                    WHERE dataset_id = %s
                """, ([col], col, col, col, dataset.dataset_id))
        
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
                    agg_exprs.append(f"SUM((data->>'{col}')::numeric) as {col}_sum")
                    result_cols.append(f"{col}_sum")
                elif func == 'mean' or func == 'avg':
                    agg_exprs.append(f"AVG((data->>'{col}')::numeric) as {col}_mean")
                    result_cols.append(f"{col}_mean")
                elif func == 'min':
                    agg_exprs.append(f"MIN((data->>'{col}')::numeric) as {col}_min")
                    result_cols.append(f"{col}_min")
                elif func == 'max':
                    agg_exprs.append(f"MAX((data->>'{col}')::numeric) as {col}_max")
                    result_cols.append(f"{col}_max")
                elif func == 'count':
                    agg_exprs.append(f"COUNT(data->>'{col}') as {col}_count")
                    result_cols.append(f"{col}_count")
                elif func == 'std':
                    agg_exprs.append(f"STDDEV((data->>'{col}')::numeric) as {col}_std")
                    result_cols.append(f"{col}_std")
        
        # build group by query
        group_cols = ', '.join([f"data->>'{col}'" for col in groupby_cols])
        select_cols = ', '.join([f"data->>'{col}' as {col}" 
                                for col in groupby_cols])
        
        cursor = self.execute_sql(f"""
            SELECT {select_cols}, {', '.join(agg_exprs)}
            FROM dataset_records
            WHERE dataset_id = %s
            GROUP BY {group_cols}
        """, (self.dataset_id,))
        
        # create result dataset
        records = []
        for row in self.fetch_all(cursor):
            records.append(dict(row))
        
        return PostgreSQLDataset.from_records(
            records,
            connection_pool=self.database_config
        )
