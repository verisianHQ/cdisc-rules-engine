from typing import List, Union, Dict, Any, Iterator, Tuple
import uuid
import json
from contextlib import contextmanager
from psycopg2.extras import Json, execute_values
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface


class PostgreSQLDataset(DatasetInterface):
    """ PostgreSQL-backed dataset implementation. """
    
    def __init__(self, dataset_id: str = None, connection_pool=None, 
                 columns=None, table_name=None, length=None):
        self.dataset_id = dataset_id or str(uuid.uuid4())
        self.connection_pool = connection_pool
        self._columns = columns or []
        self._table_name = table_name or f"dataset_{self.dataset_id.replace('-', '_')}"
        self._length = length
        self._data = None  # lazy loaded
        self._create_dataset_entry()
    
    def _create_dataset_entry(self):
        """ Register dataset in metadata table. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO datasets (dataset_id, dataset_name) 
                        VALUES (%s, %s)
                        ON CONFLICT (dataset_name, dataset_type) DO NOTHING
                    """, (self.dataset_id, self._table_name))

    @property
    def data(self):
        """ Lazy load data when accessed. """
        if self._data is None and self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT data FROM dataset_records
                        WHERE dataset_id = %s
                        ORDER BY row_num
                    """, (self.dataset_id,))
                    self._data = [row['data'] for row in cur.fetchall()]
        return self._data

    @property
    def empty(self):
        """ Check if dataset has no records. """
        return len(self) == 0

    @property
    def columns(self):
        """ Get column names from metadata or data. """
        if not self._columns and self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # first try metadata table
                    cur.execute("""
                        SELECT column_name 
                        FROM dataset_columns 
                        WHERE dataset_id = %s 
                        ORDER BY column_index
                    """, (self.dataset_id,))
                    cols = [row['column_name'] for row in cur.fetchall()]
                    
                    if not cols:
                        # fallback to extracting from first record
                        cur.execute("""
                            SELECT data FROM dataset_records
                            WHERE dataset_id = %s
                            ORDER BY row_num LIMIT 1
                        """, (self.dataset_id,))
                        result = cur.fetchone()
                        if result and result['data']:
                            cols = list(result['data'].keys())
                            # store columns for future use
                            self._register_columns(cols)
                    
                    self._columns = cols
        return self._columns

    @columns.setter
    def columns(self, columns):
        """ Set column names and update metadata. """
        self._columns = columns
        if self.connection_pool:
            self._register_columns(columns)

    def _register_columns(self, columns: List[str]):
        """ Register columns in metadata table. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # clear existing columns
                    cur.execute("DELETE FROM dataset_columns WHERE dataset_id = %s", 
                            (self.dataset_id,))
                    # insert new columns
                    for idx, col in enumerate(columns):
                        cur.execute("""
                            INSERT INTO dataset_columns 
                            (dataset_id, column_name, column_index)
                            VALUES (%s, %s, %s)
                        """, (self.dataset_id, col, idx))

    @classmethod
    def from_dict(cls, data: dict, connection_pool=None, **kwargs):
        """ Create dataset from dictionary. """
        dataset = cls(connection_pool=connection_pool)
        if connection_pool:
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
    def from_records(cls, data: List[dict], connection_pool=None, **kwargs):
        """ Create dataset from list of records. """
        dataset = cls(connection_pool=connection_pool)
        if connection_pool and data:
            dataset._insert_records(data)
            dataset._columns = list(data[0].keys()) if data else []
            if dataset._columns:
                dataset._register_columns(dataset._columns)
        return dataset

    def _insert_records(self, records: List[dict]):
        """ Bulk insert records into database. """
        if not self.connection_pool or not records:
            return
            
        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
                values = [(self.dataset_id, idx, Json(record)) 
                         for idx, record in enumerate(records)]
                execute_values(cur, """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES %s
                """, values)
                self._length = len(records)

    @classmethod
    def get_series_values(cls, series) -> list:
        """ Extract values from series-like object. """
        if isinstance(series, list):
            return series
        elif hasattr(series, 'tolist'):
            return series.tolist()
        elif hasattr(series, 'values'):
            return list(series.values)
        return list(series)

    def __getitem__(self, item: Union[str, List[str], slice, int]):
        """ Implement column/row selection. """
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
    
    def _get_column(self, column_name: str) -> List[Any]:
        """ Get single column as list. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT data->>%s as value
                        FROM dataset_records
                        WHERE dataset_id = %s
                        ORDER BY row_num
                    """, (column_name, self.dataset_id))
                    return [row['value'] for row in cur.fetchall()]
        return []
    
    def _get_columns(self, column_names: List[str]) -> "PostgreSQLDataset":
        """ Get multiple columns as new dataset. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # build json object with selected columns
                    json_build = ', '.join([f"'{col}', data->>'{col}'" 
                                          for col in column_names])
                    
                    new_dataset_id = str(uuid.uuid4())
                    cur.execute(f"""
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT %s, row_num, 
                               json_build_object({json_build})::jsonb
                        FROM dataset_records
                        WHERE dataset_id = %s
                        ORDER BY row_num
                    """, (new_dataset_id, self.dataset_id))
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=column_names
                    )
        return PostgreSQLDataset(columns=column_names)

    def _get_rows(self, slice_obj: slice) -> 'PostgreSQLDataset':
        """ Get rows by slice. """
        start = slice_obj.start or 0
        stop = slice_obj.stop or len(self)
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    cur.execute("""
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT %s, row_num - %s, data
                        FROM dataset_records
                        WHERE dataset_id = %s 
                          AND row_num >= %s 
                          AND row_num < %s
                        ORDER BY row_num
                    """, (new_dataset_id, start, self.dataset_id, start, stop))
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=self._columns
                    )
        return PostgreSQLDataset(columns=self._columns)

    def _get_row(self, row_idx: int) -> dict:
        """ Get single row as dict. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT data FROM dataset_records
                        WHERE dataset_id = %s AND row_num = %s
                    """, (self.dataset_id, row_idx))
                    result = cur.fetchone()
                    return result['data'] if result else {}
        return {}

    def __setitem__(self, key: str, value):
        """ Set column values. """
        if not self.connection_pool:
            return
            
        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
                if isinstance(value, (list, tuple)):
                    # set column from list of values
                    for idx, val in enumerate(value):
                        cur.execute("""
                            UPDATE dataset_records
                            SET data = jsonb_set(data, %s, %s)
                            WHERE dataset_id = %s AND row_num = %s
                        """, ([key], Json(val), self.dataset_id, idx))
                else:
                    # set all rows to single value
                    cur.execute("""
                        UPDATE dataset_records
                        SET data = jsonb_set(data, %s, %s)
                        WHERE dataset_id = %s
                    """, ([key], Json(value), self.dataset_id))
                
                # update columns if new
                if key not in self._columns:
                    self._columns.append(key)
                    self._register_columns(self._columns)

    def __len__(self):
        """ Get number of rows. """
        if self._length is None and self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM dataset_records
                        WHERE dataset_id = %s
                    """, (self.dataset_id,))
                    self._length = cur.fetchone()[0]
        return self._length or 0

    def __contains__(self, item: str) -> bool:
        """ Check if column exists. """
        return item in self.columns

    def get(self, column: Union[str, List[str]], default=None):
        """ Get column with default if not exists. """
        if isinstance(column, list):
            # check all columns exist
            if all(col in self.columns for col in column):
                return self._get_columns(column)
            return default
        else:
            if column in self.columns:
                return self._get_column(column)
            return default

    def groupby(self, by: Union[str, List[str]], **kwargs):
        """ Group by columns. """
        if isinstance(by, str):
            by = [by]
        
        return GroupedDataset(self, by, **kwargs)

    def concat(self, other: Union['PostgreSQLDataset', List['PostgreSQLDataset']], 
               axis=0, **kwargs):
        """ Concatenate datasets. """
        if axis == 0:  # vertical concat
            datasets = [other] if not isinstance(other, list) else other
            if self.connection_pool:
                with self.connection_pool.get_connection() as conn:
                    with conn.cursor() as cur:
                        new_dataset_id = str(uuid.uuid4())
                        
                        # copy self first
                        cur.execute("""
                            INSERT INTO dataset_records (dataset_id, row_num, data)
                            SELECT %s, row_num, data
                            FROM dataset_records
                            WHERE dataset_id = %s
                        """, (new_dataset_id, self.dataset_id))
                        
                        # append others
                        offset = len(self)
                        for ds in datasets:
                            cur.execute("""
                                INSERT INTO dataset_records (dataset_id, row_num, data)
                                SELECT %s, row_num + %s, data
                                FROM dataset_records
                                WHERE dataset_id = %s
                            """, (new_dataset_id, offset, ds.dataset_id))
                            offset += len(ds)
                        
                        return PostgreSQLDataset(
                            dataset_id=new_dataset_id,
                            connection_pool=self.connection_pool,
                            columns=self._columns
                        )
        else:  # horizontal concat
            datasets = [other] if not isinstance(other, list) else other
            if self.connection_pool:
                with self.connection_pool.get_connection() as conn:
                    with conn.cursor() as cur:
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
                        
                        cur.execute(f"""
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
                            connection_pool=self.connection_pool,
                            columns=all_columns
                        )
        
        return PostgreSQLDataset(columns=self._columns)

    def merge(self, other: 'PostgreSQLDataset', on=None, how='inner', **kwargs):
        """ Merge datasets using sql join. """
        join_type_map = {
            'inner': 'INNER JOIN',
            'left': 'LEFT JOIN',
            'right': 'RIGHT JOIN',
            'outer': 'FULL OUTER JOIN',
            'cross': 'CROSS JOIN'
        }
        
        join_type = join_type_map.get(how, 'INNER JOIN')
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    
                    if on:
                        if isinstance(on, str):
                            on = [on]
                        join_conditions = ' AND '.join([
                            f"a.data->>'{col}' = b.data->>'{col}'" for col in on
                        ])
                    else:
                        join_conditions = '1=1'
                    
                    cur.execute(f"""
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
                        connection_pool=self.connection_pool,
                        columns=merged_columns
                    )
        
        return PostgreSQLDataset(columns=self._columns)

    def apply(self, func, axis=0, **kwargs):
        """ Apply function to dataset. """
        if self.connection_pool:
            if axis == 1:  # row-wise
                # fetch all data and apply function
                results = []
                for row in self.iterrows():
                    results.append(func(row[1]))
                
                # create new dataset with results
                new_dataset = PostgreSQLDataset(connection_pool=self.connection_pool)
                if isinstance(results[0], dict):
                    new_dataset._insert_records(results)
                else:
                    new_dataset._insert_records([{'result': r} for r in results])
                return new_dataset
            else:  # column-wise
                # apply to each column
                result_dict = {}
                for col in self.columns:
                    col_data = self._get_column(col)
                    result_dict[col] = func(col_data)
                
                return result_dict
        
        return {}

    def iterrows(self) -> Iterator[Tuple[int, dict]]:
        """ Iterate over rows. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT row_num, data FROM dataset_records
                        WHERE dataset_id = %s
                        ORDER BY row_num
                    """, (self.dataset_id,))
                    
                    for row in cur:
                        yield (row['row_num'], row['data'])

    @classmethod
    def is_series(cls, data) -> bool:
        """ Check if data is series-like. """
        return isinstance(data, (list, tuple)) or hasattr(data, '__iter__')

    def convert_to_series(self, data) -> List[Any]:
        """ Convert data to list. """
        if isinstance(data, list):
            return data
        elif hasattr(data, 'tolist'):
            return data.tolist()
        elif hasattr(data, '__iter__') and not isinstance(data, (str, dict)):
            return list(data)
        else:
            return [data] * len(self)

    def get_series_from_value(self, value) -> List[Any]:
        """ Create series of repeated value. """
        return [value] * len(self)

    def rename(self, index=None, columns=None, inplace=True):
        """ Rename columns. """
        if columns and self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # update data in place
                    for old_name, new_name in columns.items():
                        cur.execute("""
                            UPDATE dataset_records
                            SET data = data - %s || jsonb_build_object(%s, data->%s)
                            WHERE dataset_id = %s
                        """, (old_name, new_name, old_name, self.dataset_id))
                    
                    # update columns list
                    self._columns = [columns.get(c, c) for c in self._columns]
                    self._register_columns(self._columns)
        
        return self if inplace else self.copy()

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """ Drop rows or columns. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    if axis == 1 or columns:  # drop columns
                        cols_to_drop = columns or labels
                        if isinstance(cols_to_drop, str):
                            cols_to_drop = [cols_to_drop]
                        
                        # remove from data
                        for col in cols_to_drop:
                            if errors == 'raise' and col not in self._columns:
                                raise KeyError(f"Column '{col}' not found")
                            
                            cur.execute("""
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
                            cur.execute("""
                                DELETE FROM dataset_records
                                WHERE dataset_id = %s AND row_num = %s
                            """, (self.dataset_id, label))
                        
                        # reindex remaining rows
                        cur.execute("""
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
        
        return self

    def melt(self, id_vars=None, value_vars=None, var_name=None, 
             value_name="value", col_level=None):
        """ Unpivot dataset from wide to long format. """
        if not self.connection_pool:
            return PostgreSQLDataset()
        
        id_vars = id_vars or []
        value_vars = value_vars or [c for c in self.columns if c not in id_vars]
        var_name = var_name or 'variable'
        
        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
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
                            insert_values.append((new_dataset_id, row_num, Json(new_row)))
                            row_num += 1
                
                # bulk insert
                execute_values(cur, """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES %s
                """, insert_values)
                
                # new columns
                new_columns = id_vars + [var_name, value_name]
                
                return PostgreSQLDataset(
                    dataset_id=new_dataset_id,
                    connection_pool=self.connection_pool,
                    columns=new_columns,
                    length=row_num
                )

    def set_index(self, keys, **kwargs):
        """ Set index columns (stored as metadata). """
        if isinstance(keys, str):
            keys = [keys]
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # store index info in dataset metadata
                    cur.execute("""
                        UPDATE datasets
                        SET metadata = jsonb_set(
                            COALESCE(metadata, '{}'), 
                            '{index_columns}', 
                            %s
                        )
                        WHERE dataset_id = %s
                    """, (Json(keys), self.dataset_id))
        
        return self

    def filter(self, items=None, like=None, regex=None, axis=None):
        """ Filter columns by criteria. """
        if axis in (1, 'columns'):
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
        """ Get row count. """
        return len(self)

    def copy(self) -> 'PostgreSQLDataset':
        """ Create deep copy of dataset. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    
                    # copy all records
                    cur.execute("""
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT %s, row_num, data
                        FROM dataset_records
                        WHERE dataset_id = %s
                    """, (new_dataset_id, self.dataset_id))
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=self._columns.copy(),
                        length=self._length
                    )
        
        return PostgreSQLDataset(columns=self._columns.copy())

    def get_error_rows(self, results) -> 'PostgreSQLDataset':
        """ Get rows where results is true. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    
                    # filter rows where result is true
                    error_rows = []
                    for idx, (row_num, row_data) in enumerate(self.iterrows()):
                        if idx < len(results) and results[idx]:
                            error_rows.append((new_dataset_id, len(error_rows), 
                                             Json(row_data)))
                    
                    if error_rows:
                        execute_values(cur, """
                            INSERT INTO dataset_records (dataset_id, row_num, data)
                            VALUES %s
                        """, error_rows[:1000])  # limit to 1000
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=self._columns,
                        length=min(len(error_rows), 1000)
                    )
        
        return PostgreSQLDataset(columns=self._columns)

    def equals(self, other: 'PostgreSQLDataset') -> bool:
        """ Check if datasets are equal. """
        if len(self) != len(other) or self.columns != other.columns:
            return False
        
        if self.connection_pool:
            # compare data row by row
            for (idx1, row1), (idx2, row2) in zip(self.iterrows(), other.iterrows()):
                if row1 != row2:
                    return False
            return True
        
        return False

    def where(self, cond, other=None, **kwargs):
        """ Filter rows by condition. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    
                    # if cond is string, use as sql where clause
                    if isinstance(cond, str):
                        cur.execute(f"""
                            INSERT INTO dataset_records (dataset_id, row_num, data)
                            SELECT %s, 
                                   ROW_NUMBER() OVER (ORDER BY row_num) - 1,
                                   data
                            FROM dataset_records
                            WHERE dataset_id = %s AND ({cond})
                        """, (new_dataset_id, self.dataset_id))
                    else:
                        # cond is list of booleans
                        filtered_rows = []
                        for idx, (row_num, row_data) in enumerate(self.iterrows()):
                            if idx < len(cond) and cond[idx]:
                                filtered_rows.append((new_dataset_id, 
                                                    len(filtered_rows), 
                                                    Json(row_data)))
                        
                        if filtered_rows:
                            execute_values(cur, """
                                INSERT INTO dataset_records (dataset_id, row_num, data)
                                VALUES %s
                            """, filtered_rows)
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=self._columns
                    )
        
        return PostgreSQLDataset(columns=self._columns)

    @classmethod
    def cartesian_product(cls, left: 'PostgreSQLDataset', 
                         right: 'PostgreSQLDataset') -> 'PostgreSQLDataset':
        """ Create cartesian product of two datasets. """
        return left.merge(right, how='cross')

    def sort_values(self, by: Union[str, List[str]], ascending=True, **kwargs):
        """ Sort dataset by columns. """
        if isinstance(by, str):
            by = [by]
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    new_dataset_id = str(uuid.uuid4())
                    
                    # build order by clause
                    order_parts = []
                    for col in by:
                        direction = 'ASC' if ascending else 'DESC'
                        order_parts.append(f"(data->>'{col}') {direction}")
                    order_clause = ', '.join(order_parts)
                    
                    cur.execute(f"""
                        INSERT INTO dataset_records (dataset_id, row_num, data)
                        SELECT %s,
                               ROW_NUMBER() OVER (ORDER BY {order_clause}) - 1,
                               data
                        FROM dataset_records
                        WHERE dataset_id = %s
                        ORDER BY {order_clause}
                    """, (new_dataset_id, self.dataset_id))
                    
                    return PostgreSQLDataset(
                        dataset_id=new_dataset_id,
                        connection_pool=self.connection_pool,
                        columns=self._columns,
                        length=self._length
                    )
        
        return self

    def is_column_sorted_within(self, group: Union[str, List[str]], column: str) -> bool:
        """ Check if column is sorted within groups. """
        if isinstance(group, str):
            group = [group]
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # use window function to check ordering
                    group_cols = ', '.join([f"data->>'{g}'" for g in group])
                    
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1
                            FROM (
                                SELECT data->>%s as col_val,
                                       LAG(data->>%s) OVER (
                                           PARTITION BY {group_cols}
                                           ORDER BY row_num
                                       ) as prev_val
                                FROM dataset_records
                                WHERE dataset_id = %s
                            ) t
                            WHERE prev_val IS NOT NULL 
                              AND prev_val > col_val
                        )
                    """, (column, column, self.dataset_id))
                    
                    return not cur.fetchone()[0]
        
        return True

    def min(self, axis=0, skipna=True, **kwargs):
        """ Get minimum values. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    if axis == 0:  # column-wise
                        result = {}
                        for col in self.columns:
                            cur.execute("""
                                SELECT MIN((data->>%s)::numeric)
                                FROM dataset_records
                                WHERE dataset_id = %s
                                  AND data ? %s
                            """, (col, self.dataset_id, col))
                            result[col] = cur.fetchone()[0]
                        return result
                    else:  # row-wise
                        results = []
                        for row_num, row_data in self.iterrows():
                            numeric_vals = [v for v in row_data.values() 
                                          if isinstance(v, (int, float))]
                            results.append(min(numeric_vals) if numeric_vals else None)
                        return results
        
        return {}

    def reset_index(self, drop=False, **kwargs):
        """ Reset row numbers to sequential. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    if not drop:
                        # save current row_num as index column
                        cur.execute("""
                            UPDATE dataset_records
                            SET data = jsonb_set(data, '{index}', to_jsonb(row_num))
                            WHERE dataset_id = %s
                        """, (self.dataset_id,))
                        
                        if 'index' not in self._columns:
                            self._columns.insert(0, 'index')
                            self._register_columns(self._columns)
                    
                    # resequence row numbers
                    cur.execute("""
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
        
        return self

    def fillna(self, value=None, method=None, axis=None, 
               inplace=False, limit=None, downcast=None):
        """ Fill null values. """
        dataset = self if inplace else self.copy()
        
        if dataset.connection_pool:
            with dataset.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    if value is not None:
                        # fill with specific value
                        for col in dataset.columns:
                            cur.execute("""
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
                            cur.execute(f"""
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

    def get_grouped_size(self, by: Union[str, List[str]], **kwargs):
        """ Get size of each group. """
        if isinstance(by, str):
            by = [by]
        
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    group_cols = ', '.join([f"data->>'{col}' as {col}" for col in by])
                    
                    cur.execute(f"""
                        SELECT {group_cols}, COUNT(*) as size
                        FROM dataset_records
                        WHERE dataset_id = %s
                        GROUP BY {', '.join([f"data->>'{col}'" for col in by])}
                    """, (self.dataset_id,))
                    
                    # return as new dataset
                    records = []
                    for row in cur.fetchall():
                        record = {col: row[col] for col in by}
                        record['size'] = row['size']
                        records.append(record)
                    
                    result = PostgreSQLDataset.from_records(
                        records, 
                        connection_pool=self.connection_pool
                    )
                    return result
        
        return PostgreSQLDataset()

    def to_dict(self, orient='records'):
        """ Export to dictionary format. """
        if self.connection_pool:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    if orient == 'records':
                        cur.execute("""
                            SELECT data 
                            FROM dataset_records 
                            WHERE dataset_id = %s 
                            ORDER BY row_num
                        """, (self.dataset_id,))
                        return [row['data'] for row in cur.fetchall()]
                    
                    elif orient == 'list':
                        result = {}
                        for col in self.columns:
                            result[col] = self._get_column(col)
                        return result
                    
                    elif orient == 'series':
                        result = {}
                        for col in self.columns:
                            result[col] = self._get_column(col)
                        return result
                    
                    elif orient == 'split':
                        return {
                            'index': list(range(len(self))),
                            'columns': self.columns,
                            'data': [list(row[1].values()) 
                                   for row in self.iterrows()]
                        }
                    
                    elif orient == 'index':
                        return {row[0]: row[1] for row in self.iterrows()}
        
        return {} if orient != 'records' else []


class GroupedDataset:
    """ Helper class for grouped operations. """
    
    def __init__(self, dataset: PostgreSQLDataset, groupby_cols: List[str], **kwargs):
        self.dataset = dataset
        self.groupby_cols = groupby_cols
        self.kwargs = kwargs
    
    def agg(self, func_dict: Dict[str, Union[str, List[str]]]):
        """ Aggregate using multiple functions. """
        if not self.dataset.connection_pool:
            return PostgreSQLDataset()
        
        with self.dataset.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
                # build aggregation expressions
                agg_exprs = []
                result_cols = list(self.groupby_cols)
                
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
                group_cols = ', '.join([f"data->>'{col}'" for col in self.groupby_cols])
                select_cols = ', '.join([f"data->>'{col}' as {col}" 
                                       for col in self.groupby_cols])
                
                cur.execute(f"""
                    SELECT {select_cols}, {', '.join(agg_exprs)}
                    FROM dataset_records
                    WHERE dataset_id = %s
                    GROUP BY {group_cols}
                """, (self.dataset.dataset_id,))
                
                # create result dataset
                records = []
                for row in cur.fetchall():
                    records.append(dict(row))
                
                return PostgreSQLDataset.from_records(
                    records,
                    connection_pool=self.dataset.connection_pool
                )
    
    def sum(self):
        """ Sum numeric columns. """
        numeric_cols = self._get_numeric_columns()
        return self.agg({col: 'sum' for col in numeric_cols})
    
    def mean(self):
        """ Average numeric columns. """
        numeric_cols = self._get_numeric_columns()
        return self.agg({col: 'mean' for col in numeric_cols})
    
    def size(self):
        """ Get size of each group. """
        return self.dataset.get_grouped_size(self.groupby_cols)
    
    def _get_numeric_columns(self):
        """ Identify numeric columns. """
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