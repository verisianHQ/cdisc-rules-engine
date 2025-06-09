import csv
import io
import json

class PostgreSQLBulkLoader:
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
    
    def load_csv(self, file_path: str, dataset_id: str, **kwargs):
        """ CSV loading using COPY. """
        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
                # first pass -> detect schema
                with open(file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    columns = reader.fieldnames
                    
                    # register columns
                    for idx, col in enumerate(columns):
                        cur.execute("""
                            INSERT INTO dataset_columns 
                            (dataset_id, column_name, column_index)
                            VALUES (%s, %s, %s)
                        """, (dataset_id, col, idx))
                
                # second pass: bulk load using COPY
                with open(file_path, 'r') as f:
                    # transform CSV to JSONB format
                    reader = csv.DictReader(f)

                    copy_sql = """
                        COPY dataset_records (dataset_id, row_num, data)
                        FROM STDIN WITH (FORMAT csv, HEADER false)
                    """
                    
                    buffer = io.StringIO()
                    for row_num, row in enumerate(reader):
                        # convert row to JSONB
                        json_data = json.dumps(row).replace('"', '""')
                        buffer.write(f'{dataset_id},{row_num},"{json_data}"\n')
                    
                    buffer.seek(0)
                    cur.copy_expert(copy_sql, buffer)
    
    def load_parquet(self, file_path: str, dataset_id: str):
        """ Load Parquet files using pyarrow and bulk insert. """
        import pyarrow.parquet as pq
        
        # read parquet file
        table = pq.read_table(file_path)
        
        # convert to record batches for streaming
        with self.connection_pool.get_connection() as conn:
            with conn.cursor() as cur:
                # register columns
                for idx, field in enumerate(table.schema):
                    cur.execute("""
                        INSERT INTO dataset_columns 
                        (dataset_id, column_name, column_type, column_index)
                        VALUES (%s, %s, %s, %s)
                    """, (dataset_id, field.name, str(field.type), idx))
                
                # bulk insert
                insert_sql = """
                    INSERT INTO dataset_records (dataset_id, row_num, data)
                    VALUES %s
                """
                
                # process in batches
                batch_size = 10000
                for batch_num, batch in enumerate(table.to_batches(batch_size)):
                    records = []
                    for row_idx, row in enumerate(batch.to_pandas().iterrows()):
                        row_num = batch_num * batch_size + row_idx
                        row_data = row[1].to_dict()
                        records.append((dataset_id, row_num, json.load(row_data)))
                    
                    # use execute_values for better performance
                    from psycopg2.extras import execute_values
                    execute_values(cur, insert_sql, records)