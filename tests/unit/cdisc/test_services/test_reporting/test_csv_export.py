from unittest.mock import MagicMock

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.services.reporting.csv_report import CSV_HEADER, CsvReport


def _result(core_id, results):
    return RuleValidationResult(
        rule={"core_id": core_id, "executability": "Fully Executable", "actions": [], "authorities": []},
        results=results,
    )


def test_csv_export_rows():
    results = [
        _result(
            "CORE-000002",
            [
                {
                    "dataset": "ae.xpt",
                    "variables": ["AESTDY", "DOMAIN"],
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "errors": [{"row": 10, "value": {"AESTDY": None, "DOMAIN": "AE"}, "dataset": "ae.xpt"}],
                    "message": "AESTDY is null",
                }
            ],
        ),
        _result(
            "CORE-000001",
            [
                {
                    "dataset": "ae.xpt",
                    "variables": [],
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "errors": [{"dataset": "ae.xpt"}],
                    "message": "Dataset-level issue",
                }
            ],
        ),
        _result(
            "CORE-000003",
            [
                {
                    "dataset": "dm.xpt",
                    "executionStatus": ExecutionStatus.EXECUTION_ERROR.value,
                    "errors": [{"dataset": "dm.xpt", "message": "boom"}],
                    "message": "rule execution error",
                },
                {
                    "dataset": "ex.xpt",
                    "executionStatus": ExecutionStatus.EXECUTION_ERROR.value,
                    "errors": [],
                    "message": "rule execution error",
                },
            ],
        ),
    ]
    header, rows = CsvReport([], ["ae.xpt"], results, 1.0, MagicMock()).get_export()
    assert header == CSV_HEADER
    assert rows == [
        ["CORE-000001", "AE", "", "", "", "Dataset-level issue"],
        ["CORE-000002", "AE", "10", "AESTDY", "", "AESTDY is null"],
        ["CORE-000002", "AE", "10", "DOMAIN", "AE", "AESTDY is null"],
        ["CORE-000003", "DM", "", "EXECUTION_ERROR", "boom", "rule execution error"],
        ["CORE-000003", "EX", "", "EXECUTION_ERROR", "rule execution error", "rule execution error"],
    ]
