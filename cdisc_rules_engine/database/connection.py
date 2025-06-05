import logging
import os

from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import SimpleConnectionPool

from contextlib import contextmanager
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """ PostgreSQL connection manager with connection pooling. """

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv(
            'DATABASE_URL',
            'postgresql://user:password@localhost:5432/cdisc_rules'
        )
        self.pool = SimpleConnectionPool(
            1, 20,
            self.connection_string
        )

    @contextmanager
    def get_cursor(self, cursor_factory=RealDictCursor):
        """ Get a database cursor from the pool. """
        conn = self.pool.getconn()
        try:
            with conn:
                with conn.cursor(cursor_factory=cursor_factory) as cur:
                    yield cur
        finally:
            self.pool.putconn(conn)

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """ Execute a SELECT query and return results. """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """ Execute an INSERT/UPDATE/DELETE query and return affected rows. """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """ Execute a query multiple times with different parameters. """
        with self.get_cursor() as cur:
            cur.executemany(query, params_list)

    def close(self):
        """ Close all connections in the pool. """
        self.pool.closeall()

db = DatabaseConnection()