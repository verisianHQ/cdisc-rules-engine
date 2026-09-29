import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService

from .helpers import (
    assert_operation_constant,
    setup_sql_operations,
)


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
        ({"TIVERS": [], "IETESTCD": []}, "TIVERS", True),
        ({"TIVERS": [None, "2"], "IETESTCD": ["A", "B"]}, "tivers", False),
    ],
)
def test_variable_is_null(data, target, expected):
    operation = setup_sql_operations("variable_is_null", target, data)
    result = operation.execute()
    assert result.subtype == "Bool"
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "target, rule_table_data, expected",
    [
        ("dataset_label", {"dataset_label": [None, ""]}, True),
        ("dataset_label", {"dataset_label": ["Adverse Events", None]}, False),
        ("dataset_label", {"dataset_label": [" ", None]}, True),
        ("variable_length", {"variable_length": [None, None]}, True),
        ("variable_length", {"variable_length": [8, None]}, False),
        ("define_variable_has_no_data", {"define_variable_has_no_data": ["Yes", "No"]}, False),
        ("library_variable_label", {"dataset_label": ["Adverse Events"]}, True),
        ("AETERM", {"AETERM": [None, None]}, False),
    ],
)
def test_variable_is_null_rule_table_column(target, rule_table_data, expected, sdtm_standards_context):
    operation = setup_sql_operations(
        "variable_is_null",
        target,
        {"AETERM": ["A", "B"]},
        standards_context=sdtm_standards_context,
    )
    PostgresQLDataService.add_test_dataset(
        operation.data_service,
        table_name="rule_table",
        column_data=rule_table_data,
        standards_context=sdtm_standards_context,
    )
    operation.params.table = "rule_table"
    result = operation.execute()
    assert result.params is None
    assert_operation_constant(operation, result, expected)
