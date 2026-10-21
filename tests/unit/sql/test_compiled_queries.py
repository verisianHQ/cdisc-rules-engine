import copy
from unittest.mock import patch

from cdisc_rules_engine.compiler import compiler as compiler_module
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor

from test_compiled_rules import AE, StandardsContext, condition, rule


def run(rule_definition):
    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [TestDataset.from_records("ae", AE)], StandardsContext()
    )
    data_service.datasets = [
        BaseDatasetMetadata(**{k: v for k, v in data_service.datasets[0].__dict__.items()}, domain="AE")
    ]
    statements = []
    execute_sql = data_service.pgi.execute_sql

    def record(query, *args, **kwargs):
        statements.append(query)
        return execute_sql(query, *args, **kwargs)

    data_service.pgi.execute_sql = record
    try:
        results = SQLRulesEngine(data_service, StandardsContext()).sql_validate_single_rule(
            copy.deepcopy(rule_definition)
        )
        return next(iter(results.values()))[0], statements
    finally:
        data_service.pgi.close()


EVERY_ROW = {"all": [condition("non_empty", "USUBJID")]}


def test_a_record_rule_reports_every_failing_row():
    reported, _ = run(rule(EVERY_ROW, output_variables=["AESEQ"]))
    assert len(reported["errors"]) == 6
    assert reported["message"] == "AE check failed"


def test_record_errors_are_capped_and_the_message_says_how_many_there_were():
    with patch.object(compiler_module, "MAX_ERRORS_PER_DATASET", 2):
        reported, _ = run(rule(EVERY_ROW, output_variables=["AESEQ"]))
    assert [error["row"] for error in reported["errors"]] == [1, 2]
    assert reported["message"] == "AE check failed (2 of 6 errors shown)"


def test_a_dataset_rule_reports_its_first_failing_row_once():
    reported, statements = run(rule(EVERY_ROW, sensitivity="Dataset", output_variables=["AESEQ"]))
    assert [error["value"] for error in reported["errors"]] == [{"AESEQ": 1}]
    assert any("LIMIT 1" in statement for statement in statements)


def grouped(*grouping_variables):
    definition = rule(EVERY_ROW, sensitivity="Group", output_variables=["USUBJID", "AESEQ"])
    definition["grouping_variables"] = list(grouping_variables)
    return definition


def test_a_group_rule_reports_the_first_failing_row_of_each_group():
    reported, _ = run(grouped("USUBJID"))
    assert [(error["value"]["USUBJID"], error["row"]) for error in reported["errors"]] == [
        ("S1", 1),
        ("S2", 3),
        ("S3", 5),
    ]


def test_a_grouping_variable_the_dataset_lacks_puts_every_row_in_one_group():
    reported, _ = run(grouped("USUBJID", "--NOTAVARIABLE"))
    assert [error["row"] for error in reported["errors"]] == [1, 3, 5]


def test_filtering_groups_by_a_dataset_name_the_dataset_lacks_reports_nothing():
    reported, _ = run(grouped("filter_by_dataset"))
    assert reported["errors"] == []


def test_a_postgres_error_is_reported_as_an_error_in_the_check_that_caused_it():
    reported, _ = run(rule({"all": [condition("non_empty", "AETERM"), condition("matches_regex", "AETERM", "(")]}))
    assert reported["executionStatus"] == "execution_error"
    assert reported["errors"][0]["error"] == "SQL error in matches_regex operator"


def with_operations(operation_variables):
    return patch.object(SQLRuleProcessor, "perform_rule_operations", return_value=operation_variables)


def test_operation_values_are_selected_with_the_failing_rows():
    operations = {
        "$terms": SqlOperationResult(
            query="SELECT value FROM (VALUES ('HEADACHE'), (NULL)) AS terms(value)", type="collection", subtype="Char"
        ),
        "$seq_terms": SqlOperationResult(
            query="SELECT value FROM (VALUES (1, 'ONE')) AS terms(seq, value) WHERE seq = $1",
            type="collection",
            subtype="Char",
            params={"$1": "AESEQ"},
        ),
    }
    with with_operations(operations):
        reported, statements = run(rule(EVERY_ROW, output_variables=["AESEQ", "$terms", "$seq_terms"]))
    values = [error["value"] for error in reported["errors"][:2]]
    assert values == [
        {"AESEQ": 1, "$terms": ["HEADACHE"], "$seq_terms": ["ONE"]},
        {"AESEQ": 2, "$terms": ["HEADACHE"], "$seq_terms": None},
    ]
    assert sum("ARRAY(" in statement for statement in statements) == 1, "selected in the rule's own query"


def test_an_operation_value_the_query_cannot_select_is_evaluated_per_row():
    operations = {"$pair": SqlOperationResult(query="SELECT 1 AS first, 2 AS second", type="constant", subtype="Num")}
    with with_operations(operations):
        reported, _ = run(rule(EVERY_ROW, output_variables=["$pair"]))
    assert reported["errors"][0]["value"] == {"$pair": 1}
