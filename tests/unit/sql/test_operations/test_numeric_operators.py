import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import RuleExecutionError
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext

from .helpers import (
    TEST_TABLE_NAME,
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, "max", 18),
        ({"values": [11, 12, 12, 5, 18, 9]}, "min", 5),
        ({"values": [11, 12, 12, 5, 17, 9]}, "mean", 11),
    ],
)
def test_sql_maximum(data, op, expected):
    operation = setup_sql_operations(op, "values", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "max",
            [
                {"params": {"$1": 1}, "value": [12.0]},
                {"params": {"$1": 2}, "value": [18.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "min",
            [
                {"params": {"$1": 1}, "value": [11.0]},
                {"params": {"$1": 2}, "value": [5.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 13, 4, 18, 9]},
            "mean",
            [
                {"params": {"$1": 1}, "value": [12.0]},
                {"params": {"$1": 2}, "value": [11.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
    ],
)
def test_sql_maximum_grouping(data, op, expected):
    operation = setup_sql_operations(op, "values", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, op, regex, expected",
    [
        ({"values": [11, 12, 105, 5, 180, 9]}, "max", r"^\d{2}$", 12),
        ({"values": [11, 12, 105, 5, 180, 9]}, "min", r"^\d{3}$", 105),
        ({"values": [11, 12, 105, 5, 180, 9]}, "mean", r"^\d$", 7),
        ({"values": ["2023-01-15", "2023-02", "2022-12-31", "2024"]}, "max", r"^\d{4}-\d{2}-\d{2}$", "2023-01-15"),
    ],
)
def test_sql_numeric_regex(data, op, regex, expected):
    operation = setup_sql_operations(op, "values", data, extra_config={"regex": regex})
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


def test_sql_numeric_regex_grouping():
    data = {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 105, 5, 180, 9]}
    operation = setup_sql_operations("max", "values", data, extra_config={"grouping": ["grp"], "regex": r"^\d{2}$"})
    result = operation.execute()
    assert_operation_parameterized_constant(
        operation,
        result,
        [
            {"params": {"$1": 1}, "value": [12.0]},
            {"params": {"$1": 2}, "value": [None]},
            {"params": {"$1": 3}, "value": [None]},
        ],
    )


def _setup_over_previous_operation(previous_operation, op, extra_config={}):
    params = SqlOperationParams(
        domain=TEST_TABLE_NAME,
        target="$previous",
        standards_context=DefaultStandardsContext(),
        previous_operations={"$previous": previous_operation.execute()},
        **extra_config,
    )
    return SqlOperationsFactory.get_service(op, params, previous_operation.data_service)


def _filtered_grouped_record_count(
    data, ignore_empty_filtered_groups, target=None, grouping=("grp",), filter={"flag": "Y"}, **extra_config
):
    return setup_sql_operations(
        "record_count",
        target,
        data,
        extra_config={
            "grouping": list(grouping),
            "filter": filter,
            "ignore_empty_filtered_groups": ignore_empty_filtered_groups,
            **extra_config,
        },
    )


@pytest.mark.parametrize(
    "op, expected",
    [
        ("max", 3),
        ("min", 1),
        ("mean", 2),
    ],
)
def test_sql_numeric_over_grouped_record_count(op, expected):
    data = {"grp": ["A", "A", "B", "C", "C", "C"], "values": [1, 2, 3, 4, 5, 6]}
    record_count = setup_sql_operations("record_count", None, data, extra_config={"grouping": ["grp"]})
    operation = _setup_over_previous_operation(record_count, op)
    result = operation.execute()
    assert result.params is None
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "op, ignore_empty_filtered_groups, expected",
    [
        ("max", False, 2),
        ("min", False, 0),
        ("mean", False, 1),
        ("max", True, 2),
        ("min", True, 1),
        ("mean", True, 1.5),
    ],
)
def test_sql_numeric_over_filtered_grouped_record_count(op, ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "A", "B", "C"], "flag": ["Y", "Y", "N", "Y"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups)
    operation = _setup_over_previous_operation(record_count, op)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups, expected", [(False, 0), (True, None)])
def test_sql_numeric_over_grouped_record_count_all_groups_filtered_out(ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "B"], "flag": ["N", "N"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups)
    operation = _setup_over_previous_operation(record_count, "max")
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups", [False, True])
def test_sql_numeric_over_grouped_record_count_ignore_without_filter(ignore_empty_filtered_groups):
    data = {"grp": ["A", "A", "B"], "flag": ["Y", "Y", "N"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups, filter=None)
    operation = _setup_over_previous_operation(record_count, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, 1)


@pytest.mark.parametrize("ignore_empty_filtered_groups, expected", [(False, 0), (True, 2)])
def test_sql_numeric_over_regex_grouped_record_count(ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "A", "B"], "code": ["X1", "X2", "Y1"]}
    record_count = _filtered_grouped_record_count(
        data, ignore_empty_filtered_groups, target="code", filter=None, regex="^X"
    )
    operation = _setup_over_previous_operation(record_count, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups, expected", [(False, 0), (True, 2)])
def test_sql_numeric_over_filtered_grouped_record_count_null_group(ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "A", None], "flag": ["Y", "Y", "N"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups)
    operation = _setup_over_previous_operation(record_count, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups, expected", [(False, 0), (True, 1)])
def test_sql_numeric_over_filtered_multi_grouped_record_count(ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "A", "A", "B"], "sub": [1, 1, 2, 1], "flag": ["Y", "Y", "N", "Y"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups, grouping=("grp", "sub"))
    operation = _setup_over_previous_operation(record_count, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups, expected", [(False, 3), (True, 2)])
def test_sql_record_count_over_filtered_grouped_record_count(ignore_empty_filtered_groups, expected):
    data = {"grp": ["A", "A", "B", "C"], "flag": ["Y", "Y", "N", "Y"]}
    record_count = _filtered_grouped_record_count(data, ignore_empty_filtered_groups)
    operation = _setup_over_previous_operation(record_count, "record_count")
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


def test_sql_filtered_grouped_record_count_ignore_keeps_per_row_values():
    data = {"grp": ["A", "A", "B", "C"], "flag": ["Y", "Y", "N", "Y"]}
    operation = _filtered_grouped_record_count(data, ignore_empty_filtered_groups=True)
    result = operation.execute()
    assert_operation_parameterized_constant(
        operation,
        result,
        [
            {"params": {"$1": "A"}, "value": [2]},
            {"params": {"$1": "B"}, "value": [0]},
            {"params": {"$1": "C"}, "value": [1]},
        ],
    )


@pytest.mark.parametrize("ignore_empty_filtered_groups", [False, True])
def test_sql_numeric_over_filtered_grouped_max_ignores_empty_groups(ignore_empty_filtered_groups):
    data = {"grp": ["A", "A", "B"], "values": [5, 7, 3], "flag": ["Y", "Y", "N"]}
    grouped_max = setup_sql_operations(
        "max",
        "values",
        data,
        extra_config={
            "grouping": ["grp"],
            "filter": {"flag": "Y"},
            "ignore_empty_filtered_groups": ignore_empty_filtered_groups,
        },
    )
    operation = _setup_over_previous_operation(grouped_max, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, 7)


def test_sql_numeric_over_grouped_max():
    data = {"grp": [1, 1, 2, 2, 3], "values": [5, 7, 3, 1, 9]}
    grouped_max = setup_sql_operations("max", "values", data, extra_config={"grouping": ["grp"]})
    operation = _setup_over_previous_operation(grouped_max, "min")
    result = operation.execute()
    assert_operation_constant(operation, result, 3)


def test_sql_numeric_over_ungrouped_operation_raises():
    data = {"grp": ["A", "A", "B"], "values": [1, 2, 3]}
    record_count = setup_sql_operations("record_count", None, data)
    operation = _setup_over_previous_operation(record_count, "max")
    with pytest.raises(RuleExecutionError, match="itself grouped"):
        operation.execute()


def test_sql_numeric_over_previous_operation_with_grouping_raises():
    data = {"grp": ["A", "A", "B"], "values": [1, 2, 3]}
    record_count = setup_sql_operations("record_count", None, data, extra_config={"grouping": ["grp"]})
    operation = _setup_over_previous_operation(record_count, "max", extra_config={"grouping": ["grp"]})
    with pytest.raises(RuleExecutionError, match="cannot combine"):
        operation.execute()
