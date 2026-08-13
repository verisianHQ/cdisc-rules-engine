from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest  # noqa

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.exceptions.custom_exceptions import RuleResourceExceededError
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine


class TestSQLRulesEngineBudget:
    def _make_engine(self, config_service=None):
        data_service = MagicMock()
        data_service.pgi = MagicMock()
        standards_context = MagicMock()
        engine = SQLRulesEngine(
            data_service=data_service,
            standards_context=standards_context,
            config_service=config_service,
        )
        return engine

    def test_get_rule_budget_uses_rule_limits(self):
        config_service = MagicMock()
        config_service.get_rule_max_execution_time_seconds.return_value = 300.0
        config_service.get_rule_max_memory_mb.return_value = 2048.0

        engine = self._make_engine(config_service)
        rule = {
            "execution_limits": {
                "max_execution_time_seconds": 60.0,
                "max_memory_mb": 512.0,
            }
        }

        assert engine._get_rule_budget(rule) == (60.0, 512.0)

    def test_get_rule_budget_falls_back_to_config(self):
        config_service = MagicMock()
        config_service.get_rule_max_execution_time_seconds.return_value = 300.0
        config_service.get_rule_max_memory_mb.return_value = 2048.0

        engine = self._make_engine(config_service)
        rule = {}

        assert engine._get_rule_budget(rule) == (300.0, 2048.0)

    def test_get_rule_budget_partial_override(self):
        config_service = MagicMock()
        config_service.get_rule_max_execution_time_seconds.return_value = 300.0
        config_service.get_rule_max_memory_mb.return_value = 2048.0

        engine = self._make_engine(config_service)
        rule = {"execution_limits": {"max_execution_time_seconds": 60.0}}

        assert engine._get_rule_budget(rule) == (60.0, 2048.0)

    @patch("cdisc_rules_engine.sql_rules_engine.RuleExecutionBudget")
    def test_validate_single_dataset_returns_resource_limit_on_timeout(self, mock_budget_cls):
        config_service = MagicMock()
        config_service.get_rule_max_execution_time_seconds.return_value = 300.0
        config_service.get_rule_max_memory_mb.return_value = 2048.0

        engine = self._make_engine(config_service)
        engine.validate_rule = MagicMock(side_effect=RuleResourceExceededError("too slow", "time"))

        metadata = SimpleNamespace(
            name="ae",
            filename="ae.xpt",
            domain="AE",
        )
        result = engine.validate_single_dataset({}, metadata, [])

        assert len(result) == 1
        assert result[0]["executionStatus"] == ExecutionStatus.RESOURCE_LIMIT.value
        assert "too slow" in result[0]["message"]
