from cdisc_rules_engine.enums.base_enum import BaseEnum


class ExecutionStatus(BaseEnum):
    SUCCESS = "success"
    ISSUE_REPORTED = "issue_reported"
    SKIPPED = "skipped"
    EXECUTION_ERROR = "execution_error"
