from unittest.mock import patch

from cdisc_rules_engine.config.config import ConfigService


class TestConfigService:
    def test_default_rule_budget_values(self):
        with patch("os.getenv", return_value=None):
            # Reset singleton to ensure fresh instance
            ConfigService._instance = None
            config = ConfigService()
            assert config.get_rule_max_execution_time_seconds() == 300.0
            assert config.get_rule_max_memory_mb() == 2048.0

    def test_rule_budget_values_from_env(self):
        env_values = {
            "RULE_MAX_EXECUTION_TIME_SECONDS": "600",
            "RULE_MAX_MEMORY_MB": "4096",
        }

        def mock_getenv(key, default=None):
            return env_values.get(key, default)

        with patch("os.getenv", side_effect=mock_getenv):
            ConfigService._instance = None
            config = ConfigService()
            assert config.get_rule_max_execution_time_seconds() == 600.0
            assert config.get_rule_max_memory_mb() == 4096.0

    def test_rule_budget_values_invalid_env_uses_default(self):
        def mock_getenv(key, default=None):
            if key == "RULE_MAX_EXECUTION_TIME_SECONDS":
                return "not-a-number"
            if key == "RULE_MAX_MEMORY_MB":
                return ""
            return default

        with patch("os.getenv", side_effect=mock_getenv):
            ConfigService._instance = None
            config = ConfigService()
            assert config.get_rule_max_execution_time_seconds() == 300.0
            assert config.get_rule_max_memory_mb() == 2048.0
