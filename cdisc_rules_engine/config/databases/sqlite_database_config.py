import sqlite3
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any


class SQLiteDatabaseConfig:
    """Manages SQLite database connections with thread-local storage."""
    
    def __init__(self):
        self._local = threading.local()
        self._database_path: Optional[str] = None
        self._is_in_memory: bool = False
        self._connection_params: Dict[str, Any] = {
            'check_same_thread': True,  # do not allow connection sharing between threads
            'timeout': 100.0,
            'isolation_level': None  # Autocommit mode
        }
    
    def initialise(self, database_path: str = None, in_memory: bool = False, **kwargs):
        """Initialise the database configuration."""
        if in_memory:
            self._database_path = ':memory:'
            self._is_in_memory = True
            if 'check_same_thread' not in kwargs:
                kwargs['check_same_thread'] = False
        else:
            if not database_path:
                raise ValueError("database_path is required when in_memory=False")
            self._database_path = database_path
            self._is_in_memory = False
        
        self._connection_params.update(kwargs)

        if self._is_in_memory:
            self._setup_in_memory_tables()
    
    def _setup_in_memory_tables(self):
        """Setup required tables for in-memory database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # create the required tables for SQLDatasetBase
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    table_name TEXT,
                    metadata TEXT DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dataset_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT,
                    row_num INTEGER,
                    data TEXT,
                    UNIQUE(dataset_id, row_num),
                    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dataset_columns (
                    dataset_id TEXT,
                    column_name TEXT,
                    column_index INTEGER,
                    column_type TEXT,
                    PRIMARY KEY (dataset_id, column_name),
                    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dataset_records_dataset_row 
                ON dataset_records(dataset_id, row_num)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dataset_columns_dataset 
                ON dataset_columns(dataset_id)
            """)
            
            conn.commit()
    
    def _get_thread_connection(self) -> sqlite3.Connection:
        """Get or create a connection for the current thread."""
        if self._is_in_memory and hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.execute("SELECT 1")
            except sqlite3.ProgrammingError:
                self._local.connection = None
        
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            if not self._database_path:
                raise RuntimeError("Database not initialised. Call initialise() first.")
            
            # create a new connection for this thread
            self._local.connection = sqlite3.connect(
                self._database_path,
                **self._connection_params
            )
            
            # enable row factory for dict-like access
            self._local.connection.row_factory = sqlite3.Row
            
            # recreate tables if needed
            if self._is_in_memory:
                self._ensure_tables_exist(self._local.connection)
        
        return self._local.connection
    
    def _ensure_tables_exist(self, conn: sqlite3.Connection):
        """Ensure tables exist in the connection."""
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='datasets'
        """)
        
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    table_name TEXT,
                    metadata TEXT DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dataset_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT,
                    row_num INTEGER,
                    data TEXT,
                    UNIQUE(dataset_id, row_num),
                    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dataset_columns (
                    dataset_id TEXT,
                    column_name TEXT,
                    column_index INTEGER,
                    column_type TEXT,
                    PRIMARY KEY (dataset_id, column_name),
                    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dataset_records_dataset_row 
                ON dataset_records(dataset_id, row_num)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dataset_columns_dataset 
                ON dataset_columns(dataset_id)
            """)
            
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Get a database connection for the current thread."""
        conn = self._get_thread_connection()
        try:
            yield conn
            # check if in auto-commit mode, if not then commit.
            if self._connection_params.get('isolation_level') is not None:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
    
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
    
    @property
    def is_in_memory(self) -> bool:
        """Check if this is an in-memory database."""
        return self._is_in_memory

