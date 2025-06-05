import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SQLExecutor:
    """ Executes SQL-based validation rules. """
    
    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params
    
    def setup_database(self) -> None:
        """ Initialise database schema. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                # Execute the schema creation SQL
                with open('cdisc_rules_engine/sql/schema/001_core_tables.sql', 'r') as f:
                    cur.execute(f.read())
                conn.commit()
    
    def execute_query(self, query: str, params: Dict[str, Any]) -> List[Dict]:
        """ Execute a validation query and return results. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
    
    def load_datasets(self, study_id: str, dataset_paths: List[str]) -> List[Dict]:
        """ Load datasets using the DataLoader. """ 
        from cdisc_rules_engine.ingestion.loader import DataLoader
        loader = DataLoader(self.conn_params)
        
        dataset_ids = []
        for path in dataset_paths:
            dataset_id = loader.load_dataset(
                file_path=path,
                study_id=study_id,
                standard="SDTM",  # This should be parameterised : TODO: ADaM functionality
                version="3.4"     # This should be parameterised
            )
            dataset_ids.append({
                'dataset_id': dataset_id,
                'path': path,
                'name': path.split('/')[-1].split('.')[0].upper()
            })
        
        return dataset_ids