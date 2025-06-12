import sqlite3
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any


class SQLiteDatabaseConfig:
    """ Manages SQLite database connections with thread-local storage. """
    
    def __init__(self):
        self._local = threading.local()
        self._database_path: Optional[str] = None
        self._connection_params: Dict[str, Any] = {
            'check_same_thread': False,  # allow connection sharing between threads
            'timeout': 30.0,
            'isolation_level': None  # Autocommit mode
        }
    
    def initialise(self, database_path: str, **kwargs):
        """Initialise the database configuration."""
        self._database_path = database_path
        self._connection_params.update(kwargs)
    
    def _get_thread_connection(self) -> sqlite3.Connection:
        """Get or create a connection for the current thread."""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            if not self._database_path:
                raise RuntimeError("Database not initialised. Call initialise() first.")
            
            # Create a new connection for this thread
            self._local.connection = sqlite3.connect(
                self._database_path,
                **self._connection_params
            )
            # Enable row factory for dict-like access
            self._local.connection.row_factory = sqlite3.Row
            
        return self._local.connection
    
    @contextmanager
    def get_connection(self):
        """Get a database connection for the current thread."""
        conn = self._get_thread_connection()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        # Note: No commit here since we're in autocommit mode (isolation_level=None)
    
    def close_thread_connection(self):
        """Close the connection for the current thread."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
    
    def close_all(self):
        """
            Close all connections.
            Note:
            - This only closes the current thread's connection.
            - Other threads must close their own connections.
        """
        self.close_thread_connection()
    
    @property
    def database_path(self) -> Optional[str]:
        """Get the current database path."""
        return self._database_path