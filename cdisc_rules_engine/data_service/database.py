import logging
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from os import getenv
from typing import Optional

import psycopg2.pool
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from cdisc_rules_engine.exceptions.custom_exceptions import DatabaseConnectionError
from cdisc_rules_engine.services import logger

load_dotenv()


@dataclass
class DatabaseConfigPostgres:
    host = getenv("DATABASE_HOST")
    port = int(getenv("DATABASE_PORT") or 5432)
    database = getenv("DATABASE_NAME")
    user = getenv("DATABASE_USER")
    password = getenv("DATABASE_PASSWORD")
    min_connections: int = 1
    max_connections: int = 10
    connect_timeout: int = 10  # seconds


@dataclass
class DatabaseConfigPGServer:
    min_connections: int = 1
    max_connections: int = 10
    connect_timeout: int = 10  # seconds


class DatabasePostgres:
    """Database connection management with connection pooling"""

    _pgserver_instance = None
    _pgserver_tempdir = None
    _pgserver_dburi = None

    # a failed reset is retried this many times in total, waiting RESET_BACKOFF_SECONDS * attempt in between
    RESET_ATTEMPTS = 3
    RESET_BACKOFF_SECONDS = 0.5

    def __init__(self, config: DatabaseConfigPostgres | DatabaseConfigPGServer = DatabaseConfigPostgres()):
        self.config = config
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        # whether any query has gone through the pool since it was opened, i.e. whether a reset would free anything
        self._used_since_reset = False
        self._init_pool()

    def _init_pool(self):
        """Initialise connection pool"""
        self._pool = self._create_pool()
        logger.info("Database connection pool initialised successfully")

    def _create_pool(self) -> psycopg2.pool.SimpleConnectionPool:
        """Open a new connection pool, leaving the current one (if any) untouched."""
        try:
            if isinstance(self.config, DatabaseConfigPGServer):
                logging.getLogger("pgserver").setLevel(logging.WARNING)
                if DatabasePostgres._pgserver_instance is None:
                    self.setup_pg_server()
                else:
                    self.dburi = DatabasePostgres._pgserver_dburi
                return psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    self.dburi,
                    connect_timeout=self.config.connect_timeout,
                )
            try:
                return psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password,
                    connect_timeout=self.config.connect_timeout,
                )
            except psycopg2.OperationalError as e:
                raise DatabaseConnectionError(
                    f"Could not connect to Postgres database '{self.config.database}' at "
                    f"{self.config.host}:{self.config.port}: {str(e).strip()}\n"
                    "Check the DATABASE_* settings in your .env file, "
                    "or run with --in-memory-postgres to use a temporary in-memory instance."
                ) from e
        except DatabaseConnectionError:
            raise
        except Exception as e:
            logger.error(f"Failed to initialise connection pool: {e}")
            raise

    def setup_pg_server(self):
        # Importing pgserver here so it is only loaded when an in-memory instance is requested
        try:
            import pgserver
        except ImportError as e:
            raise DatabaseConnectionError(
                "The in-memory Postgres instance requires the 'pgserver' package. "
                "Install it with `pip install -r requirements.txt`, or set the DATABASE_* settings "
                "in your .env file to use an existing Postgres database."
            ) from e

        DatabasePostgres._pgserver_tempdir = tempfile.mkdtemp()
        DatabasePostgres._pgserver_instance = pgserver.get_server(
            DatabasePostgres._pgserver_tempdir, cleanup_mode="delete"
        )
        DatabasePostgres._pgserver_dburi = DatabasePostgres._pgserver_instance.get_uri()
        self.dburi = DatabasePostgres._pgserver_dburi

    @contextmanager
    def get_connection_and_cursor(self, dict_cursor: bool = True):
        conn = self._pool.getconn()
        self._used_since_reset = True
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield conn, cursor
            cursor.close()
        finally:
            self._pool.putconn(conn)

    def close_pool(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")

    def reset_pool(self) -> bool:
        """
        Swap the pool for a fresh one, returning whether it was swapped.
        A Postgres backend keeps the memory it peaked at for as long as it lives, even once it has been
        freed internally (e.g. after planning a very large statement), so a long-lived connection can hold
        on to tens of GB and starve the server's page cache for the rest of the run.
        A new connection means a new backend, which starts small.

        The reset is only an optimisation, so it never leaves the run worse off:
          - nothing happens if no query has used the pool since it was opened (nothing to free)
          - the new pool is opened *before* the old one is closed, so a failed or interrupted
            reconnect leaves the old, working pool in place (and end-of-run cleanup can still use it)
          - a failed reconnect (e.g. "too many clients" on a busy server) is retried a few times,
            then given up on with a warning, carrying on with the old pool
        """
        if not self._used_since_reset:
            return False

        new_pool = None
        for attempt in range(1, self.RESET_ATTEMPTS + 1):
            try:
                new_pool = self._create_pool()
                break
            except Exception as e:
                # first line only: the rest is startup advice (check .env, ...) that doesn't apply mid-run
                reason = (str(e).strip().splitlines() or [type(e).__name__])[0]
                if attempt == self.RESET_ATTEMPTS:
                    logger.warning(
                        f"Could not open a fresh database connection after {attempt} attempts ({reason}); "
                        "carrying on with the current one"
                    )
                    return False
                logger.debug(f"Database reconnect attempt {attempt} failed ({reason}); retrying")
                time.sleep(self.RESET_BACKOFF_SECONDS * attempt)

        old_pool, self._pool = self._pool, new_pool
        self._used_since_reset = False
        try:
            if old_pool:
                old_pool.closeall()
        except Exception as e:
            # the new pool is already in place, so the run is fine; at worst the old one lingers until it's dropped
            logger.warning(f"Could not close the previous database connection pool: {e}")
        logger.debug("Database connection pool reset")
        return True
