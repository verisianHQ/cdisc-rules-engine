from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.services.reporting.excel_report import ExcelReport


def report(*results_per_rule):
    instance = ExcelReport.__new__(ExcelReport)
    instance._item_type = "list"
    instance._results = list(results_per_rule)
    return instance


class FakeResult:
    def __init__(self, core_id, execution_status, results):
        self.id = core_id
        self.cdisc_rule_id = ""
        self.fda_rule_id = ""
        self.message = "a message"
        self.execution_status = execution_status
        self.results = results


def skipped(core_id, *reasons):
    return FakeResult(
        core_id,
        ExecutionStatus.SKIPPED.value,
        [{"executionStatus": ExecutionStatus.SKIPPED.value, "message": reason} for reason in reasons],
    )


def succeeded(core_id, errors=()):
    return FakeResult(
        core_id,
        ExecutionStatus.SUCCESS.value,
        [{"executionStatus": ExecutionStatus.SUCCESS.value, "message": None, "errors": list(errors)}],
    )


def errored(dataset, error="SQL error in equal_to operator"):
    return {
        "executionStatus": ExecutionStatus.EXECUTION_ERROR.value,
        "dataset": dataset,
        "message": "SQL operator execution error",
        "errors": [{"dataset": dataset, "error": error, "message": "column does not exist"}],
    }


def skipped_dataset(dataset):
    return {
        "executionStatus": ExecutionStatus.SKIPPED.value,
        "message": f"Rule skipped - doesn't apply to domain for rule id=CORE-000001, dataset={dataset}",
    }


def test_a_skipped_rule_reports_its_reason():
    rows = report(skipped("CORE-000022", "Rule skipped - variables not in dataset: AESER")).get_rules_report_data()
    assert rows[0][-2] == "SKIPPED"
    assert rows[0][-1] == "Rule skipped - variables not in dataset: AESER"


def test_reasons_from_several_datasets_are_collapsed_and_joined():
    rows = report(skipped("CORE-000022", "no AESER", "no AESER", "class not in scope")).get_rules_report_data()
    assert rows[0][-1] == "no AESER; class not in scope"


def test_a_rule_that_ran_has_no_reason():
    rows = report(succeeded("CORE-000022")).get_rules_report_data()
    assert rows[0][-2] == "SUCCESS"
    assert rows[0][-1] == ""


def test_reasons_recorded_per_dataset_are_grouped_by_reason():
    rows = report(
        skipped(
            "CORE-000001",
            "Rule skipped - doesn't apply to domain for rule id=CORE-000001, dataset=lb",
            "Rule skipped - doesn't apply to class for rule id=CORE-000001, dataset=ae",
            "Rule skipped - doesn't apply to domain for rule id=CORE-000001, dataset=qs",
            "rule execution error",
        )
    ).get_rules_report_data()
    assert rows[0][-1] == (
        "Rule skipped - doesn't apply to domain (LB, QS); "
        "Rule skipped - doesn't apply to class (AE); "
        "rule execution error"
    )


def test_a_rule_that_raised_issues_is_issue_reported():
    rows = report(succeeded("CORE-000022", errors=[{"row": 1}])).get_rules_report_data()
    assert rows[0][-2] == "ISSUE REPORTED"
    assert rows[0][-1] == ""


def test_a_rule_that_failed_reports_execution_error_with_only_the_errors_as_reason():
    result = FakeResult("CORE-000001", ExecutionStatus.SKIPPED.value, [errored("ae.xpt"), skipped_dataset("lb")])
    rows = report(result).get_rules_report_data()
    assert rows[0][-2] == "EXECUTION ERROR"
    assert rows[0][-1] == "SQL operator execution error: SQL error in equal_to operator: column does not exist (AE)"


def test_execution_error_outranks_success():
    result = succeeded("CORE-000001")
    result.results.append(errored("ae.xpt"))
    assert report(result).get_rules_report_data()[0][-2] == "EXECUTION ERROR"


def test_execution_error_outranks_issues_on_another_dataset():
    result = succeeded("CORE-000001", errors=[{"row": 1}])
    result.results.append(errored("ae.xpt"))
    rows = report(result).get_rules_report_data()
    assert rows[0][-2] == "EXECUTION ERROR"
    assert rows[0][-1] == "SQL operator execution error: SQL error in equal_to operator: column does not exist (AE)"


def test_issues_outrank_success_on_another_dataset():
    result = succeeded("CORE-000001", errors=[{"row": 1}])
    result.results.append({"executionStatus": ExecutionStatus.SUCCESS.value, "message": None, "errors": []})
    assert report(result).get_rules_report_data()[0][-2] == "ISSUE REPORTED"


def test_success_outranks_skipped_datasets():
    result = succeeded("CORE-000001")
    result.results.append(skipped_dataset("lb"))
    rows = report(result).get_rules_report_data()
    assert rows[0][-2] == "SUCCESS"
    assert rows[0][-1] == ""
