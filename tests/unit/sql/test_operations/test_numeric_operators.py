import pytest

from .helpers import (
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
