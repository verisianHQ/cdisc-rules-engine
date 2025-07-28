from dataclasses import dataclass
from contextlib import contextmanager
from typing import Optional
import psycopg2.pool
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration"""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    min_connections: int = 1
    max_connections: int = 10


class Database:
    """Database connection management with connection pooling"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        self._init_pool()

    def _init_pool(self):
        """Initialise connection pool"""
        try:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )
            logger.info("Database connection pool initialised successfully")
        except Exception as e:
            logger.error(f"Failed to initialise connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        if self._pool:
            try:
                conn = self._pool.getconn()
                yield conn
            finally:
                if conn:
                    self._pool.putconn(conn)

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """Get a cursor from a pooled connection"""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    def close_pool(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")
