from unittest.mock import MagicMock, call, patch

import psycopg2
import pytest

from cdisc_rules_engine.data_service.database import DatabaseConfigPostgres, DatabasePostgres
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface

POOL_CLASS = "cdisc_rules_engine.data_service.database.psycopg2.pool.SimpleConnectionPool"
SLEEP = "cdisc_rules_engine.data_service.database.time.sleep"
TOO_MANY_CLIENTS = psycopg2.OperationalError("FATAL:  sorry, too many clients already")


def make_db(pool_class, *pools_or_errors):
    """A DatabasePostgres whose first pool is `old`, with later pool creations returning/raising the rest in turn."""
    old = MagicMock(name="old_pool")
    pool_class.side_effect = [old, *pools_or_errors]
    db = DatabasePostgres(DatabaseConfigPostgres())
    with db.get_connection_and_cursor():  # a rule ran a query, so there is something to reset
        pass
    return db, old


@patch(SLEEP)
@patch(POOL_CLASS)
def test_reset_opens_the_new_pool_before_closing_the_old_one(pool_class, _sleep):
    events = []
    new = MagicMock(name="new_pool")
    db, old = make_db(pool_class, new)
    pool_class.side_effect = lambda *args, **kwargs: events.append("opened new") or new
    old.closeall.side_effect = lambda: events.append("closed old")

    assert db.reset_pool() is True

    assert events == ["opened new", "closed old"]
    assert db._pool is new
    new.closeall.assert_not_called()


@patch(POOL_CLASS)
def test_reset_is_skipped_when_nothing_used_the_connection(pool_class):
    old = MagicMock(name="old_pool")
    pool_class.side_effect = [old]
    db = DatabasePostgres(DatabaseConfigPostgres())  # e.g. a rule that was skipped before running any SQL

    assert db.reset_pool() is False

    assert pool_class.call_count == 1  # only the initial pool
    old.closeall.assert_not_called()
    assert db._pool is old


@patch(SLEEP)
@patch(POOL_CLASS)
def test_a_second_reset_in_a_row_is_skipped(pool_class, _sleep):
    db, _ = make_db(pool_class, MagicMock(name="new_pool"))

    assert db.reset_pool() is True
    assert db.reset_pool() is False  # nothing has run on the fresh connection yet

    assert pool_class.call_count == 2


@patch(SLEEP)
@patch(POOL_CLASS)
def test_transient_failures_are_retried_with_backoff(pool_class, sleep):
    new = MagicMock(name="new_pool")
    db, old = make_db(pool_class, TOO_MANY_CLIENTS, TOO_MANY_CLIENTS, new)

    assert db.reset_pool() is True

    assert db._pool is new
    old.closeall.assert_called_once()
    assert sleep.call_args_list == [call(0.5), call(1.0)]


@patch(SLEEP)
@patch(POOL_CLASS)
def test_a_server_that_stays_full_does_not_end_the_run(pool_class, sleep, caplog):
    db, old = make_db(pool_class, *[TOO_MANY_CLIENTS] * DatabasePostgres.RESET_ATTEMPTS)

    assert db.reset_pool() is False  # no exception: the run carries on

    assert db._pool is old
    old.closeall.assert_not_called()
    assert sleep.call_count == DatabasePostgres.RESET_ATTEMPTS - 1
    # and the old pool still serves the next rule's queries
    with db.get_connection_and_cursor():
        pass
    old.getconn.assert_called()


@patch(SLEEP)
@patch(POOL_CLASS)
def test_a_failed_reset_is_tried_again_before_the_next_rule(pool_class, _sleep):
    new = MagicMock(name="new_pool")
    db, old = make_db(pool_class, *[TOO_MANY_CLIENTS] * DatabasePostgres.RESET_ATTEMPTS, new)

    assert db.reset_pool() is False
    assert db.reset_pool() is True  # the old connection was still in use, so it's still worth freeing

    assert db._pool is new
    old.closeall.assert_called_once()


@pytest.mark.parametrize("interruption", [KeyboardInterrupt(), SystemExit(130)])
@patch(SLEEP)
@patch(POOL_CLASS)
def test_an_interrupted_reconnect_leaves_the_old_pool_working(pool_class, sleep, interruption):
    db, old = make_db(pool_class, interruption)

    with pytest.raises(type(interruption)):
        db.reset_pool()

    # not retried, and end-of-run cleanup can still drop the run's tables through the old pool
    sleep.assert_not_called()
    assert db._pool is old
    old.closeall.assert_not_called()


@patch(SLEEP)
@patch(POOL_CLASS)
def test_failing_to_close_the_old_pool_keeps_the_new_one(pool_class, _sleep):
    new = MagicMock(name="new_pool")
    db, old = make_db(pool_class, new)
    old.closeall.side_effect = psycopg2.InterfaceError("connection already closed")

    assert db.reset_pool() is True

    assert db._pool is new


@patch(POOL_CLASS)
def test_connections_are_opened_with_a_timeout(pool_class):
    DatabasePostgres(DatabaseConfigPostgres())

    assert pool_class.call_args.kwargs["connect_timeout"] == DatabaseConfigPostgres.connect_timeout


def test_reset_connections_resets_the_pool():
    pgi = PostgresQLInterface(sql_namespace="test")
    pgi.db = MagicMock()

    pgi.reset_connections()

    pgi.db.reset_pool.assert_called_once()


def test_reset_connections_before_init_is_a_no_op():
    pgi = PostgresQLInterface(sql_namespace="test")

    pgi.reset_connections()  # no database yet, nothing to reset

    assert pgi.db is None
