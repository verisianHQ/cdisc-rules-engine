import os

from dotenv import load_dotenv

from cdisc_rules_engine.interfaces import ConfigInterface
import psutil

load_dotenv()


class ConfigService(ConfigInterface):

    _config_keys = []
    _instance = None
    # TODO: Make this configurable via env variable
    _default_dataset_size_threshold = psutil.virtual_memory().available * 0.25

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigService, cls).__new__(cls)
            # Put any initialization here.
            cls._config_keys = [
                "ENGINE_STORAGE_TYPE",
                "CACHE_TYPE",
                "REDIS_HOST_NAME",
                "REDIS_ACCESS_KEY",
                "CDISC_LIBRARY_API_KEY",
                "DATA_SERVICE_TYPE",
                "DATASET_SIZE_THRESHOLD",
                "RULE_MAX_EXECUTION_TIME_SECONDS",
                "RULE_MAX_MEMORY_MB",
            ]

        return cls._instance

    def getValue(self, key, default=None):
        if key in ConfigService._config_keys:
            return os.getenv(key)
        else:
            return default

    def get_dataset_size_threshold(self):
        try:
            return float(self.getValue("DATASET_SIZE_THRESHOLD") or self._default_dataset_size_threshold)
        except Exception:
            return self._default_dataset_size_threshold

    def get_rule_max_execution_time_seconds(self, default: float = 300.0) -> float:
        try:
            value = self.getValue("RULE_MAX_EXECUTION_TIME_SECONDS")
            if value is None or value == "":
                return default
            return float(value)
        except Exception:
            return default

    def get_rule_max_memory_mb(self, default: float = 16000.0) -> float:
        try:
            value = self.getValue("RULE_MAX_MEMORY_MB")
            if value is None or value == "":
                return default
            return float(value)
        except Exception:
            return default
