import time
from unittest.mock import MagicMock, patch

import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import RuleResourceExceededError
from cdisc_rules_engine.utilities.rule_execution_budget import RuleExecutionBudget


class TestRuleExecutionBudget:
    def test_time_budget_allows_fast_execution(self):
        with RuleExecutionBudget(max_time_seconds=10.0, check_interval_seconds=0.01):
            time.sleep(0.05)

    def test_time_budget_cancels_slow_execution(self):
        conn = MagicMock()
        budget = RuleExecutionBudget(max_time_seconds=0.05, check_interval_seconds=0.01)

        with pytest.raises(RuleResourceExceededError):
            with budget:
                budget.register_connection(conn)
                time.sleep(0.5)

        conn.cancel.assert_called_once()
        assert budget.resource_type == "time"

    @patch("cdisc_rules_engine.utilities.rule_execution_budget.psutil.Process")
    def test_memory_budget_cancels_high_usage(self, mock_process_cls):
        conn = MagicMock()
        process = MagicMock()
        process.memory_info.return_value = MagicMock(rss=10 * 1024 * 1024 * 1024)  # 10GB
        mock_process_cls.return_value = process

        budget = RuleExecutionBudget(max_memory_mb=1.0, check_interval_seconds=0.01)

        with pytest.raises(RuleResourceExceededError):
            with budget:
                budget.register_connection(conn)
                time.sleep(0.5)

        conn.cancel.assert_called_once()
        assert budget.resource_type == "memory"

    def test_no_budget_no_monitoring(self):
        with RuleExecutionBudget(max_time_seconds=None, max_memory_mb=None):
            time.sleep(0.05)
