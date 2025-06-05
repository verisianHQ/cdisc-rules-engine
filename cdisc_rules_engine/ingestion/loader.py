import json
import csv
from pathlib import Path
from typing import Optional, Dict, List, Any
import logging
import psycopg2
from psycopg2.extras import execute_batch
import pyreadstat

logger = logging.getLogger(__name__)

class DataLoader:
    """ Handles loading datasets directly into PostgreSQL. """
    
    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params
    
    def load_dataset(self, file_path: str, study_id: str,
                    standard: str, version: str) -> int:
        """ Load dataset. """
        logger.info(f"Loading dataset: {file_path}")
        
        dataset_name = Path(file_path).stem.upper()
        file_extension = Path(file_path).suffix.lower()
        
        with psycopg2.connect(**self.conn_params) as conn:
            if file_extension == ".xpt":
                return self._load_xpt_file(conn, file_path, study_id, 
                                         dataset_name, standard, version)
            elif file_extension == ".csv":
                return self._load_csv_file(conn, file_path, study_id, 
                                         dataset_name, standard, version)
            elif file_extension == ".json":
                return self._load_json_file(conn, file_path, study_id, 
                                          dataset_name, standard, version)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
    
    def _load_xpt_file(self, conn, file_path: str, study_id: str,
                      dataset_name: str, standard: str, version: str) -> int:
        """ Load XPT file. """
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat, 
            file_path, 
            chunksize=1000,
            formats_as_category=False,
            usecols=None
        )
        
        chunk_iter = reader
        first_chunk, meta = next(chunk_iter)
        
        columns = meta.column_names
        column_types = self._map_sas_types_to_pg(meta)
        
        dataset_id = self._create_dataset_table(
            conn, study_id, dataset_name, columns, 
            column_types, standard, version
        )
        
        self._insert_chunk_direct(conn, dataset_name, columns, first_chunk)
        
        for chunk, _ in chunk_iter:
            self._insert_chunk_direct(conn, dataset_name, columns, chunk)
        
        self._update_dataset_metadata(conn, dataset_id, dataset_name)
        
        return dataset_id
    
    def _load_csv_file(self, conn, file_path: str, study_id: str,
                      dataset_name: str, standard: str, version: str) -> int:
        """ Load CSV file. """
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames
            
            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 100:
                    break
            
            column_types = self._infer_column_types(sample_rows, columns)
            
            dataset_id = self._create_dataset_table(
                conn, study_id, dataset_name, columns, 
                column_types, standard, version
            )
            
            f.seek(0)
            next(csv.reader(f))
            
            with conn.cursor() as cur:
                cur.copy_expert(
                    f"""
                        COPY sdtm.{dataset_name} ({",".join(columns)})
                        FROM STDIN WITH CSV
                    """,
                    f
                )
            
            self._update_dataset_metadata(conn, dataset_id, dataset_name)
            
        return dataset_id
    
    def _load_json_file(self, conn, file_path: str, study_id: str,
                       dataset_name: str, standard: str, version: str) -> int:
        """ Load Dataset-JSON file. """
        with open(file_path, "r") as f:
            data = json.load(f)
        
        columns = [col["name"] for col in data["columns"]]
        column_types = self._map_json_types_to_pg(data["columns"])
        
        dataset_id = self._create_dataset_table(
            conn, study_id, dataset_name, columns, 
            column_types, standard, version
        )
        
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO sdtm.{dataset_name} 
                ({",".join(columns)})
                VALUES ({",".join(["%s"] * len(columns))})
            """
            
            batch_size = 1000
            rows = data.get("rows", [])
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                execute_batch(cur, insert_sql, batch, page_size=batch_size)
        
        self._update_dataset_metadata(conn, dataset_id, dataset_name)
        
        return dataset_id
    
    def _create_dataset_table(self, conn, study_id: str, dataset_name: str,
                            columns: List[str], column_types: Dict[str, str],
                            standard: str, version: str) -> int:
        """ Create a dynamic table for the dataset. """
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS sdtm.{dataset_name} CASCADE")
            
            column_defs = ["study_id VARCHAR(200)"]
            column_defs.extend([
                f"{col} {column_types.get(col, "TEXT")}"
                for col in columns
            ])
            column_defs.append("row_number SERIAL")
            
            create_sql = f"""
                CREATE TABLE sdtm.{dataset_name} (
                    {",".join(column_defs)},
                    PRIMARY KEY (study_id, row_number)
                )
            """
            cur.execute(create_sql)
            
            cur.execute(f"""
                CREATE INDEX idx_{dataset_name}_usubjid 
                ON sdtm.{dataset_name}(usubjid)
                WHERE usubjid IS NOT NULL
            """)
            
            domain = self._extract_domain_from_table(cur, dataset_name, columns)

            cur.execute(
                """
                    INSERT INTO sdtm.datasets
                    (study_id, dataset_name, dataset_path, domain, 
                    standard, standard_version)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (study_id, dataset_name)
                    DO UPDATE SET
                        modification_date = CURRENT_TIMESTAMP
                    RETURNING dataset_id
                """,
                (study_id, dataset_name, "", domain, standard, version)
            )
            
            dataset_id = cur.fetchone()[0]
            
            for i, col in enumerate(columns):
                cur.execute(
                    """
                        INSERT INTO sdtm.variable_metadata
                        (dataset_id, variable_name, variable_type, variable_order)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (dataset_id, variable_name)
                        DO UPDATE SET
                            variable_type = EXCLUDED.variable_type,
                            variable_order = EXCLUDED.variable_order
                    """,
                    (dataset_id, col, column_types.get(col, "TEXT"), i + 1)
                )
            
            conn.commit()
            return dataset_id
    
    @staticmethod
    def _insert_chunk_direct(conn, dataset_name: str, 
                           columns: List[str], chunk_data) -> None:
        """ Insert chunk data directly into table. """
        with conn.cursor() as cur:
            rows = []
            for _, row in chunk_data.iterrows():
                rows.append(tuple(row[col] for col in columns))
            
            if rows:
                insert_sql = f"""
                    INSERT INTO sdtm.{dataset_name} 
                    ({",".join(columns)})
                    VALUES ({",".join(["%s"] * len(columns))})
                """
                execute_batch(cur, insert_sql, rows, page_size=1000)
    
    @staticmethod
    def _map_sas_types_to_pg(meta) -> Dict[str, str]:
        """Map SAS data types to PostgreSQL types."""
        type_map = {}
        for i, col in enumerate(meta.column_names):
            sas_type = meta.readstat_variable_types[col]
            if sas_type == "double" or sas_type == "float":
                type_map[col] = "NUMERIC"
            elif sas_type == "string":
                length = meta.variable_storage_width.get(col, 200)
                type_map[col] = f"VARCHAR({length})"
            else:
                type_map[col] = "TEXT"
        return type_map
    
    @staticmethod
    def _map_json_types_to_pg(columns: List[Dict]) -> Dict[str, str]:
        """Map Dataset-JSON types to PostgreSQL types."""
        type_map = {}
        for col in columns:
            name = col["name"]
            dtype = col.get("dataType", "string")
            
            if dtype in ["float", "double", "decimal"]:
                type_map[name] = "NUMERIC"
            elif dtype == "integer":
                type_map[name] = "INTEGER"
            elif dtype == "boolean":
                type_map[name] = "BOOLEAN"
            elif dtype == "date":
                type_map[name] = "DATE"
            elif dtype in ["datetime", "time"]:
                type_map[name] = "TIMESTAMP"
            else:
                length = col.get("length", 200)
                type_map[name] = f"VARCHAR({length})"
        
        return type_map
    
    def _infer_column_types(self, sample_rows: List[Dict], 
                          columns: List[str]) -> Dict[str, str]:
        """ Infer PostgreSQL column types from sample data. """
        type_map = {}
        
        for col in columns:
            all_numeric = True
            all_integer = True
            max_length = 0
            
            for row in sample_rows:
                value = row.get(col, "")
                if value:
                    max_length = max(max_length, len(str(value)))
                    try:
                        float(value)
                        if "." in str(value):
                            all_integer = False
                    except ValueError:
                        all_numeric = False
                        all_integer = False
                        break
            
            if all_numeric and all_integer:
                type_map[col] = "INTEGER"
            elif all_numeric:
                type_map[col] = "NUMERIC"
            else:
                if max_length <= 50:
                    type_map[col] = "VARCHAR(50)"
                elif max_length <= 200:
                    type_map[col] = "VARCHAR(200)"
                elif max_length <= 1000:
                    type_map[col] = "VARCHAR(1000)"
                else:
                    type_map[col] = "TEXT"
        
        return type_map
    
    def _extract_domain_from_table(self, cursor, dataset_name: str, 
                                 columns: List[str]) -> Optional[str]:
        """Extract domain directly from database."""
        if "DOMAIN" in columns:
            # Domain will be populated when data is inserted
            return None
        elif dataset_name.startswith("SUPP"):
            return None
        else:
            return dataset_name[:2].upper()
    
    def _update_dataset_metadata(self, conn, dataset_id: int, 
                               dataset_name: str) -> None:
        """ Update dataset metadata after loading. """
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM sdtm.{dataset_name}")
            record_count = cur.fetchone()[0]
            
            cur.execute(
                f"""
                    SELECT DISTINCT domain 
                    FROM sdtm.{dataset_name} 
                    WHERE domain IS NOT NULL 
                    LIMIT 1
                """
            )
            result = cur.fetchone()
            domain = result[0] if result else dataset_name[:2].upper()
            
            cur.execute(
                """
                    UPDATE sdtm.datasets
                    SET record_count = %s,
                        domain = %s,
                        modification_date = CURRENT_TIMESTAMP
                    WHERE dataset_id = %s
                """,
                (record_count, domain, dataset_id)
            )
            
            conn.commit()