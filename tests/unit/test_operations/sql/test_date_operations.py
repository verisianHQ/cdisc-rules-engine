from .helpers import (
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_sql_operations,
)
import pytest
import pandas as pd


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, pd.to_datetime("2022-01-05").isoformat()),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, pd.to_datetime("2000-01-01").isoformat()),
        ({"dates": ["2023-06-15"]}, pd.to_datetime("2023-06-15").isoformat()),
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
                {"params": {"$1": 1}, "value": [pd.to_datetime("2022-01-05").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2023-01-01").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [pd.to_datetime("2001-01-01").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2010-12-12").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2010-12-12").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": [pd.to_datetime("2020-01-01").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2021-12-31").isoformat()]},
                {"params": {"$1": 3}, "value": [pd.to_datetime("2019-06-15").isoformat()]},
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
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, pd.to_datetime("2001-01-01").isoformat()),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, pd.to_datetime("1999-01-01").isoformat()),
        ({"dates": ["2023-06-15"]}, pd.to_datetime("2023-06-15").isoformat()),
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
                {"params": {"$1": 1}, "value": [pd.to_datetime("2001-01-01").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2010-12-12").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [pd.to_datetime("2001-01-01").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2010-12-12").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2010-12-12").isoformat()]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": [pd.to_datetime("2020-01-01").isoformat()]},
                {"params": {"$1": 2}, "value": [pd.to_datetime("2021-12-31").isoformat()]},
                {"params": {"$1": 3}, "value": [pd.to_datetime("2019-06-15").isoformat()]},
            ],
        ),
    ],
)
def test_min_date_grouping(data, expected):
    operation = setup_sql_operations("min_date", "dates", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)
