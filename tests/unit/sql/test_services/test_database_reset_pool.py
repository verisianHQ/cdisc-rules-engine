from unittest.mock import MagicMock, patch

from cdisc_rules_engine.data_service.database import DatabaseConfigPostgres, DatabasePostgres
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@patch("cdisc_rules_engine.data_service.database.psycopg2.pool.SimpleConnectionPool")
def test_reset_pool_closes_old_connections_and_opens_a_new_pool(pool_class):
    first_pool, second_pool = MagicMock(), MagicMock()
    pool_class.side_effect = [first_pool, second_pool]

    db = DatabasePostgres(DatabaseConfigPostgres())
    db.reset_pool()

    first_pool.closeall.assert_called_once()
    second_pool.closeall.assert_not_called()
    assert db._pool is second_pool


def test_reset_connections_resets_the_pool():
    pgi = PostgresQLInterface(sql_namespace="test")
    pgi.db = MagicMock()

    pgi.reset_connections()

    pgi.db.reset_pool.assert_called_once()


def test_reset_connections_before_init_is_a_no_op():
    pgi = PostgresQLInterface(sql_namespace="test")

    pgi.reset_connections()  # no database yet, nothing to reset

    assert pgi.db is None
