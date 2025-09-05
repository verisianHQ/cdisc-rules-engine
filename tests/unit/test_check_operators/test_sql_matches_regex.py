import pytest
from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["word", "TEST"]},
            ".*",
            [True, True],
        ),
        (
            {"target": ["word", "TEST"]},
            "[0-9].*",
            [False, False],
        ),
        (
            {"target": ["224", "abc"]},
            "^[1-9]{1}\\d*$",
            [True, False],
        ),
        (
            {"target": ["-25", "3.14"]},
            "^-?[1-9]{1}\\d*$",
            [True, False],
        ),
        (
            {"target": ["word", None, "TEST"]},
            ".*",
            [True, False, True],
        ),
        (
            {"target": [None, None]},
            "[0-9].*",
            [False, False],
        ),
    ],
)
def test_sql_matches_regex(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.matches_regex({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["word", "TEST"]},
            ".*",
            [False, False],
        ),
        (
            {"target": ["word", "TEST"]},
            "[0-9].*",
            [True, True],
        ),
        (
            {"target": ["224", "abc"]},
            "^[1-9]{1}\\d*$",
            [False, True],
        ),
        (
            {"target": ["-25", "3.14"]},
            "^-?[1-9]{1}\\d*$",
            [False, True],
        ),
        (
            {"target": ["word", None, "TEST"]},
            ".*",
            [False, False, False],
        ),
        (
            {"target": [None, None]},
            "[0-9].*",
            [False, False],
        ),
    ],
)
def test_sql_not_matches_regex(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.not_matches_regex({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)
