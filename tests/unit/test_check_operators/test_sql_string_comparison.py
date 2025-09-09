import pandas as pd
import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            # {"target": ["Att", "", None, {None}, {None, 1}, {1, 2}]},
            # [False, True, True, True, False, False],
            {"target": ["Att", "", None]},
            [False, True, True],
        ),
        (
            {"target": [1, 2, None]},
            [False, False, True],
        ),
    ],
)
def test_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.empty({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [True, False, False]),
        ({"target": [1, 2, None]}, [True, True, False]),
    ],
)
def test_non_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.non_empty({"target": "target"})
    assert_series_equals(result, expected_result)


LONGER_THAN_TEST_DATA = [
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "lll"]},
        "VAR2",
        False,
        [True, True, False],
    ),
    (
        {"target": ["Att", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A", "AB", "ABC", "ABCD"]},
        "AB",
        True,
        [False, False, True, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctta"]},
        3,
        True,
        [False, False, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctt"]},
        2,
        True,
        [True, True, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": [2, 3, 2]},
        "VAR2",
        False,
        [True, False, True],
    ),
    (
        {"target": ["", "A", "AB", "ABC"]},
        0,
        True,
        [False, True, True, True],
    ),
    (
        {"target": ["A", "", "ABC"], "VAR2": ["", "XX", ""]},
        "VAR2",
        False,
        [True, False, True],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        "$number",
        False,
        [False, True, True],
    ),
    (
        {"target": ["", "A", "AB", "ABC"]},
        "$constant",
        False,
        [False, False, True, True],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    LONGER_THAN_TEST_DATA,
)
def test_sql_longer_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.longer_than(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    LONGER_THAN_TEST_DATA,
)
def test_sql_shorter_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shorter_than_or_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, ~pd.Series(expected_result))
