import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundError

from .helpers import (
    TEST_TABLE_NAME,
    assert_operation_constant,
    setup_sql_operations,
)

RULE_TABLE_NAME = "rule_table"


@pytest.mark.parametrize(
    "data, target, expected",
    [
        ({"TIVERS": [None, None, None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": ["", "", ""], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": [None, "", None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", True),
        ({"TIVERS": [None, "2", None], "IETESTCD": ["A", "B", "C"]}, "TIVERS", False),
        ({"TIVERS": ["1", "1", "2"], "IETESTCD": ["A", "B", "C"]}, "TIVERS", False),
        ({"AESEQ": [None, None], "IETESTCD": ["A", "B"]}, "AESEQ", True),
        ({"AESEQ": [None, 2], "IETESTCD": ["A", "B"]}, "AESEQ", False),
        ({"AESEQ": [0, 0], "IETESTCD": ["A", "B"]}, "AESEQ", False),
        ({"IETESTCD": ["A", "B"]}, "TIVERS", True),
        ({"TIVERS": [" ", " "], "IETESTCD": ["A", "B"]}, "TIVERS", True),
        ({"TIVERS": [None, "  "], "IETESTCD": ["A", "B"]}, "TIVERS", True),
        ({"TIVERS": [" ", " 1 "], "IETESTCD": ["A", "B"]}, "TIVERS", False),
        ({"TIVERS": ["\t", " \n "], "IETESTCD": ["A", "B"]}, "TIVERS", True),
        ({"TIVERS": ["\t", " \n1"], "IETESTCD": ["A", "B"]}, "TIVERS", False),
        ({"TIVERS": [], "IETESTCD": []}, "TIVERS", True),
        ({"TIVERS": [None, "2"], "IETESTCD": ["A", "B"]}, "tivers", False),
    ],
)
def test_variable_is_null(data, target, expected):
    operation = setup_sql_operations("variable_is_null", target, data)
    result = operation.execute()
    assert result.subtype == "Bool"
    assert_operation_constant(operation, result, expected)


def _setup_with_rule_table(target, rule_table_data, use_rule_type_table, standards_context):
    operation = setup_sql_operations(
        "variable_is_null",
        target,
        {"AETERM": ["A", "B"]},
        standards_context=standards_context,
        extra_config={"table": RULE_TABLE_NAME, "use_rule_type_table": use_rule_type_table},
    )
    PostgresQLDataService.add_test_dataset(
        operation.data_service,
        table_name=RULE_TABLE_NAME,
        column_data=rule_table_data,
        standards_context=standards_context,
    )
    return operation


@pytest.mark.parametrize(
    "target, rule_table_data, expected",
    [
        ("dataset_label", {"dataset_label": [None, ""]}, True),
        ("dataset_label", {"dataset_label": ["Adverse Events", None]}, False),
        ("dataset_label", {"dataset_label": [" ", None]}, True),
        ("variable_length", {"variable_length": [None, None]}, True),
        ("variable_length", {"variable_length": [8, None]}, False),
        ("define_variable_has_no_data", {"define_variable_has_no_data": ["Yes", "No"]}, False),
        ("AETERM", {"AETERM": [None, None]}, True),
        ("library_variable_label", {"dataset_label": ["Adverse Events"]}, True),
    ],
)
def test_variable_is_null_use_rule_type_table(target, rule_table_data, expected, sdtm_standards_context):
    operation = _setup_with_rule_table(target, rule_table_data, True, sdtm_standards_context)
    result = operation.execute()
    assert result.params is None
    assert_operation_constant(operation, result, expected)


def test_variable_is_null_does_not_fall_back_to_rule_type_table(sdtm_standards_context):
    operation = _setup_with_rule_table(
        "dataset_label", {"dataset_label": ["Adverse Events"]}, False, sdtm_standards_context
    )
    assert_operation_constant(operation, operation.execute(), True)


def test_variable_is_null_missing_domain_raises():
    operation = setup_sql_operations(
        "variable_is_null", "AETERM", {"AETERM": ["A", "B"]}, extra_config={"table": TEST_TABLE_NAME}
    )
    operation.params.domain = "DM"
    with pytest.raises(DomainNotFoundError, match="Domain DM"):
        operation.execute()


def test_variable_is_null_missing_rule_type_table_raises():
    operation = setup_sql_operations(
        "variable_is_null",
        "dataset_label",
        {"AETERM": ["A", "B"]},
        extra_config={"table": "missing_table", "use_rule_type_table": True},
    )
    with pytest.raises(DomainNotFoundError, match="Domain missing_table"):
        operation.execute()
