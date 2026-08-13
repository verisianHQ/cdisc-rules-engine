from cdisc_rules_engine.enums.base_enum import BaseEnum


class ExecutionStatus(BaseEnum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    EXECUTION_ERROR = "execution_error"
    RESOURCE_LIMIT = "resource_limit"
    PARTIAL_SUCCESS = "partial_success"
