import pandas as pd
import pytest

from .helpers import assert_series_equals, create_sql_operators


@pytest.mark.parametrize(
    "data,operator_input,expected_result",
    [
        (
            {
                "TSVAL": ["A", "B", "C", "D"],
                "TSVAL1": ["X", "A", "Y", "Z"],
                "TSVAL2": ["M", "N", "A", None],
            },
            {"target": "TSVAL", "comparator": "A"},
            [True, True, True, False],
        ),
        (
            {
                "tsval": ["a", "b", "c"],
                "TSVAL1": ["x", "A", "z"],
            },
            {"target": "TSVAL", "comparator": "A", "case_insensitive": True},
            [True, True, False],
        ),
        (
            {
                "TSVAL": ["A", "B", "C"],
                "TSVAL1": ["X", "Y", "Z"],
                "TSVALX": ["A", "A", "A"],
            },
            {"target": "TSVAL", "comparator": "A", "regex": r"^TSVAL\d*$"},
            [True, False, False],
        ),
        (
            {
                "OTHERVAR": ["A", "B", "C"],
            },
            {"target": "TSVAL", "comparator": "A"},
            [False, False, False],
        ),
    ],
)
def test_sql_in_enumerated_columns(data, operator_input, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.in_enumerated_columns(operator_input)
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,operator_input,expected_result",
    [
        (
            {
                "TSVAL": ["A", "B", "C"],
                "TSVAL1": ["X", "A", "Y"],
                "TSVAL2": ["M", "N", "A"],
            },
            {"target": "TSVAL", "comparator": "A"},
            [False, False, False],
        ),
        (
            {
                "TSVAL": ["a", "b", "c"],
                "TSVAL1": ["x", "y", "z"],
            },
            {"target": "TSVAL", "comparator": "A", "case_insensitive": True},
            [False, True, True],
        ),
    ],
)
def test_sql_not_in_enumerated_columns(data, operator_input, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.not_in_enumerated_columns(operator_input)
    assert_series_equals(result, pd.Series(expected_result))
