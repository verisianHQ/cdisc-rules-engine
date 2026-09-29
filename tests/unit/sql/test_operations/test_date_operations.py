from .helpers import (
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_over_previous_operation,
    setup_sql_operations,
)
import pytest


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, "2022-01-05"),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, "2000-01-01"),
        ({"dates": ["2023-06-15"]}, "2023-06-15"),
    ],
)
def test_max_date(data, expected):
    operation = setup_sql_operations("max_date", "dates", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 2, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]},
            [
                {"params": {"$1": 1}, "value": ["2022-01-05"]},
                {"params": {"$1": 2}, "value": ["2023-01-01"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": ["2020-01-01"]},
                {"params": {"$1": 2}, "value": ["2021-12-31"]},
                {"params": {"$1": 3}, "value": ["2019-06-15"]},
            ],
        ),
    ],
)
def test_max_date_grouping(data, expected):
    operation = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, "2001-01-01"),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, "1999-01-01"),
        ({"dates": ["2023-06-15"]}, "2023-06-15"),
    ],
)
def test_min_date(data, expected):
    operation = setup_sql_operations("min_date", "dates", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 2, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": ["2020-01-01"]},
                {"params": {"$1": 2}, "value": ["2021-12-31"]},
                {"params": {"$1": 3}, "value": ["2019-06-15"]},
            ],
        ),
    ],
)
def test_min_date_grouping(data, expected):
    operation = setup_sql_operations("min_date", "dates", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "inner_op, outer_op, expected",
    [
        ("max_date", "min_date", "2022-01-05"),
        ("max_date", "max_date", "2023-01-01"),
        ("min_date", "max_date", "2010-12-12"),
        ("min_date", "min_date", "2001-01-01"),
    ],
)
def test_date_over_grouped_date(inner_op, outer_op, expected):
    data = {"grp": [1, 1, 2, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]}
    grouped = setup_sql_operations(inner_op, "dates", data, extra_config={"grouping": ["grp"]})
    operation = setup_over_previous_operation(grouped, outer_op)
    result = operation.execute()
    assert result.params is None
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "dates, expected",
    [
        ([None, None, "2010-12-12"], "2010-12-12"),
        ([None, "", None], ""),
    ],
)
def test_date_over_grouped_date_skips_groups_without_dates(dates, expected):
    data = {"grp": [1, 1, 2], "dates": dates}
    grouped = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp"]})
    operation = setup_over_previous_operation(grouped, "min_date")
    assert_operation_constant(operation, operation.execute(), expected)


@pytest.mark.parametrize("ignore_empty_filtered_groups", [False, True])
def test_date_over_filtered_grouped_date(ignore_empty_filtered_groups):
    data = {"grp": [1, 1, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12"], "flag": ["Y", "N", "N"]}
    grouped = setup_sql_operations(
        "max_date",
        "dates",
        data,
        extra_config={
            "grouping": ["grp"],
            "filter": {"flag": "Y"},
            "ignore_empty_filtered_groups": ignore_empty_filtered_groups,
        },
    )
    operation = setup_over_previous_operation(grouped, "min_date")
    assert_operation_constant(operation, operation.execute(), "2001-01-01")


def test_date_over_grouped_date_regex():
    data = {"grp": [1, 1, 2, 2, 3], "dates": ["2001-01-01", "2023-03-05", "2010-12-12", "2023-01-01", "2024-06-01"]}
    grouped = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp"]})
    operation = setup_over_previous_operation(grouped, "max_date", extra_config={"regex": "^2023"})
    assert_operation_constant(operation, operation.execute(), "2023-03-05")


def test_date_over_grouped_date_nested_grouping():
    data = {"grp": [1, 1, 1, 2], "sub": [1, 1, 2, 1], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]}
    grouped = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp", "sub"]})
    operation = setup_over_previous_operation(grouped, "min_date", extra_config={"grouping": ["grp"]})
    assert_operation_parameterized_constant(
        operation,
        operation.execute(),
        [
            {"params": {"$1": 1}, "value": ["2010-12-12"]},
            {"params": {"$1": 2}, "value": ["2023-01-01"]},
        ],
    )


def test_record_count_over_grouped_date_counts_groups_with_dates():
    data = {"grp": [1, 1, 2, 3], "dates": ["2001-01-01", None, None, "2010-12-12"]}
    grouped = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp"]})
    operation = setup_over_previous_operation(grouped, "record_count")
    assert_operation_constant(operation, operation.execute(), 2)
