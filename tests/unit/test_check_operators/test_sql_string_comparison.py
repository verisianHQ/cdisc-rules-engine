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
        {"target": ["C", "A", "AB", "ABC"]},
        0,
        True,
        [True, True, True, True],
    ),
    (
        {"target": ["A", "ABC"], "VAR2": ["", "XX"]},
        "VAR2",
        False,
        [True, True],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        "$number",
        False,
        [False, True, True],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        "$constant",
        False,
        [False, True, True],
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


LONGER_THAN_OR_EQUAL_TO_TEST_DATA = [
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": ["A", "Bd", "ll"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": ["ABC", "XYZ", "123"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A", "BB", "CCC"], "VAR2": ["ABCD", "EFGH", "IJKL"]},
        "VAR2",
        False,
        [False, False, False],
    ),
    (
        {"target": ["A", "AB", "ABC", "ABCD"]},
        "AB",
        True,
        [False, True, True, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctta"]},
        3,
        True,
        [True, True, True],
    ),
    (
        {"target": ["At", "Btt", "C"]},
        2,
        True,
        [True, True, False],
    ),
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": [3, 2, 4]},
        "VAR2",
        False,
        [True, True, False],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        0,
        True,
        [True, True, True],
    ),
    (
        {"target": ["A", "AB"], "VAR2": ["", "XY"]},
        "VAR2",
        False,
        [True, True],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        "$constant",
        False,
        [True, True, True],
    ),
    (
        {"target": ["ABC", "AB", "A"]},
        "$number",
        False,
        [True, True, True],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    LONGER_THAN_OR_EQUAL_TO_TEST_DATA,
)
def test_sql_longer_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.longer_than_or_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    LONGER_THAN_OR_EQUAL_TO_TEST_DATA,
)
def test_sql_shorter_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shorter_than(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))


HAS_EQUAL_LENGTH_TEST_DATA = [
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": ["Add", "Bee", "Cat"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctta"], "VAR2": ["A", "Bd", "lll"]},
        "VAR2",
        False,
        [False, False, False],
    ),
    (
        {"target": ["A", "AB", "ABC", "ABCD"]},
        "AB",
        True,
        [False, True, False, False],
    ),
    (
        {"target": ["Att", "Btt", "Ctta"]},
        3,
        True,
        [True, True, False],
    ),
    (
        {"target": ["At", "Btt", "Ct"]},
        2,
        True,
        [True, False, True],
    ),
    (
        {"target": ["Att", "Btt", "Ctt"], "VAR2": [3, 3, 3]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["C", "A", "AB"]},
        0,
        True,
        [False, False, False],
    ),
    (
        {"target": ["A", "AB"], "VAR2": ["X", "YZ"]},
        "VAR2",
        False,
        [True, True],
    ),
    (
        {"target": ["A", "AB", "ABC"]},
        "$constant",
        False,
        [True, False, False],
    ),
    (
        {"target": ["ABC", "AB", "A"]},
        "$number",
        False,
        [False, False, True],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    HAS_EQUAL_LENGTH_TEST_DATA,
)
def test_sql_has_equal_length(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_equal_length(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    HAS_EQUAL_LENGTH_TEST_DATA,
)
def test_sql_has_not_equal_length(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.has_not_equal_length(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, ~pd.Series(expected_result))
