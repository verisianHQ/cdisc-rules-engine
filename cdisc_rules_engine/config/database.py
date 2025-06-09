from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.pool

class DatabaseConfig:
    def __init__(self):
        self.min_connections = 2
        self.max_connections = 20
        self.connection_pool = None
        
    def initialise_pool(self, config):
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            self.min_connections,
            self.max_connections,
            host=config.get('DB_HOST', 'localhost'),
            port=config.get('DB_PORT', 5432),
            database=config.get('DB_NAME', 'cdisc_rules'),
            user=config.get('DB_USER'),
            password=config.get('DB_PASSWORD'),
            cursor_factory=RealDictCursor
        )
    
    @contextmanager
    def get_connection(self):
        if self.connection_pool:
            connection = self.connection_pool.getconn()
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                self.connection_pool.putconn(connection)
        else:
