import pytest

from .helpers import assert_series_equals, create_sql_operators

shares_at_least_one_element_with_test_data = [
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["B,C", "D,E", "F,G"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["X,Y", "Y,Z", "Z,W"]},
        "target",
        "comparator",
        False,
        [False, False, False],
    ),
    (
        {"target": ["A", "B", "C"], "comparator": ["A,X", "B,Y", "C,Z"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": [None, "", "A,B"], "comparator": ["X,Y", "Z,W", "B,C"]},
        "target",
        "comparator",
        False,
        [False, False, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": [None, "", "F,G"]},
        "target",
        "comparator",
        False,
        [False, False, True],
    ),
]

shares_exactly_one_element_with_test_data = [
    (
        {"target": ["A", "B", "C"], "comparator": ["A,X", "B,Y", "C,Z"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["A,X", "C,Y", "E,Z"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["A,B", "C,D", "E,F"]},
        "target",
        "comparator",
        False,
        [False, False, False],
    ),
    (
        {"target": ["A", "B,C", "D,E,F"], "comparator": ["A,B", "C,D", "E,F,G"]},
        "target",
        "comparator",
        False,
        [True, True, False],
    ),
    (
        {"target": [None, "", "A"], "comparator": ["X,Y", "Z,W", "A,B"]},
        "target",
        "comparator",
        False,
        [False, False, True],
    ),
]

shares_no_elements_with_test_data = [
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["X,Y", "Y,Z", "Z,W"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["B,C", "D,E", "F,G"]},
        "target",
        "comparator",
        False,
        [False, False, False],
    ),
    (
        {"target": ["A", "B", "C"], "comparator": ["X", "Y", "Z"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
    (
        {"target": ["A,B", "C,D", "E,F"], "comparator": ["A", "C", "X"]},
        "target",
        "comparator",
        False,
        [False, False, True],
    ),
    (
        {"target": [None, "", "A,B"], "comparator": ["X,Y", "Z,W", "C,D"]},
        "target",
        "comparator",
        False,
        [True, True, True],
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,expected_result",
    shares_at_least_one_element_with_test_data,
)
def test_shares_at_least_one_element_with(data, target, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.shares_at_least_one_element_with(
        {
            "target": target,
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,expected_result",
    shares_exactly_one_element_with_test_data,
)
def test_shares_exactly_one_element_with(data, target, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.shares_exactly_one_element_with(
        {
            "target": target,
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,expected_result",
    shares_no_elements_with_test_data,
)
def test_shares_no_elements_with(data, target, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.shares_no_elements_with(
        {
            "target": target,
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)
