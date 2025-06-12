import psycopg2.pool
from psycopg2.extras import RealDictCursor

from contextlib import contextmanager
from typing import Optional, Dict, Any


class PostgreSQLDatabaseConfig:
    """ Manages PostgreSQL database connections with thread-local storage. """
    
    def __init__(self):
        self.min_connections = 2
        self.max_connections = 20
        self._database_path: Optional[str] = None 
        self._connection_params: Dict[str, Any] = {
            "host": "",
            "port": 0,
            "user": "",
            "password": ""
        }
        self.connection_pool = None
    
    def initialise(self, database_path: str, **config_params):
        """Initialise the database configuration."""
        self._database_path = database_path
        self._connection_params.update(config_params)
        
        required_params = ['host', 'port', 'user', 'password']
        missing_params = [param for param in required_params if param not in config_params]
        if missing_params:
            raise ValueError(f"Missing required parameters: {missing_params}")
        else:
            self._connection_params.update(config_params)
        
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self._connection_params['host'],
                port=self._connection_params['port'],
                database=self._database_path,
                user=self._connection_params['user'],
                password=self._connection_params['password'],
                cursor_factory=RealDictCursor,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialise PostgreSQL connection pool: {e}")
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the PostgreSQL pool."""
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialised")
        
        connection = None
        try:
            connection = self.connection_pool.getconn()
            if connection.closed:
                # connection is closed, get a new one
                self.connection_pool.putconn(connection, close=True)
                connection = self.connection_pool.getconn()
            
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    def close_all(self):
        """Close all connections."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
            print("PostgreSQL connection pool closed")
    
    @property
    def database_path(self) -> Optional[str]:
        """Get the current database path."""
        return self._database_path