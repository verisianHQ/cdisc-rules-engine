from typing import List

class QueryOptimiser:
    """ Optimise PostgreSQL queries. """
    
    @staticmethod
    def create_indexes(connection, dataset_id: str, columns: List[str]):
        """ Create indexes for frequently queried columns. """
        with connection.cursor() as cur:
            for column in columns:
                index_name = f"idx_{dataset_id}_{column}".replace('-', '_')
                cur.execute(f"""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                    ON dataset_records ((data->>%s))
                    WHERE dataset_id = %s
                """, (column, dataset_id))
    
    @staticmethod
    def analyse_dataset(connection, dataset_id: str):
        """ Update PostgreSQL statistics for query planning. """
        with connection.cursor() as cur:
            cur.execute("ANALYZE dataset_records WHERE dataset_id = %s", 
                       (dataset_id,))
    
    @staticmethod
    def create_materialised_view(connection, dataset_id: str, 
                                view_name: str, query: str):
        """ Create materialised view for complex queries. """
        with connection.cursor() as cur:
            cur.execute(f"""
                CREATE MATERIALISED VIEW {view_name} AS
                {query}
            """)
            
            # create indexes on materialised view
            cur.execute(f"""
                CREATE INDEX ON {view_name} (dataset_id)
            """)