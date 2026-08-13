from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.exceptions.custom_exceptions import RuleResourceExceededError
from cdisc_rules_engine.utilities.rule_execution_budget import RuleExecutionBudget


class TestPostgresQLInterfaceBudget:
    def _make_interface(self):
        interface = PostgresQLInterface.__new__(PostgresQLInterface)
        interface.db = MagicMock()
        interface.active_budget = None
        return interface

    @patch.object(PostgresQLInterface, "_apply_statement_timeout")
    def test_execute_sql_converts_query_canceled_with_active_budget(self, _mock_timeout):
        interface = self._make_interface()
        budget = RuleExecutionBudget(max_time_seconds=300.0, check_interval_seconds=0.01)
        interface.active_budget = budget

        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = psycopg2.errors.QueryCanceled("canceling statement due to statement timeout")

        with patch.object(interface.db, "get_connection_and_cursor") as mock_cm:
            mock_cm.return_value.__enter__ = lambda self: (conn, cursor)
            mock_cm.return_value.__exit__ = lambda self, *args: None

            with pytest.raises(RuleResourceExceededError):
                with budget:
                    budget.register_connection(conn)
                    interface.execute_sql("SELECT 1")

    def test_execute_sql_does_not_convert_query_canceled_without_budget(self):
        interface = self._make_interface()

        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = psycopg2.errors.QueryCanceled("canceling statement")

        with patch.object(interface.db, "get_connection_and_cursor") as mock_cm:
            mock_cm.return_value.__enter__ = lambda self: (conn, cursor)
            mock_cm.return_value.__exit__ = lambda self, *args: None

            with pytest.raises(psycopg2.errors.QueryCanceled):
                interface.execute_sql("SELECT 1")
