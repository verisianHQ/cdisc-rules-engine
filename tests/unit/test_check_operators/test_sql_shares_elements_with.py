import pytest
import pandas as pd

from .helpers import create_sql_operators

SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA = [
    # Operation variable vs operation variable tests only
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$constant"},
        pd.Series([True], dtype=bool),  # True because list [A,B] contains A which matches constant A
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        pd.Series([True], dtype=bool),  # True because list shares elements with itself
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$list"},
        pd.Series([True], dtype=bool),  # True because constant A is in list [A,B]
    ),
]

SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA = [
    # Operation variable vs operation variable tests only
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        pd.Series([True], dtype=bool),  # True because exactly one element (A) is shared
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        pd.Series([False], dtype=bool),  # False because list [A,B] has 2 elements, not exactly one shared
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$constant"},
        pd.Series(
            [True], dtype=bool
        ),  # True because exactly one element (A) is shared between list [A,B] and constant A
    ),
]

SHARES_NO_ELEMENTS_TEST_DATA = [
    # Operation variable vs operation variable tests only
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$date"},
        pd.Series([True], dtype=bool),  # True because constant A doesn't match date 2025-09-09
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$date"},
        pd.Series([True], dtype=bool),  # True because list [A,B] doesn't contain date 2025-09-09
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$list"},
        pd.Series([False], dtype=bool),  # False because list shares all elements with itself
    ),
    (
        {"dummy": ["value"]},
        {"target": "$date", "comparator": "$constant"},
        pd.Series([True], dtype=bool),  # True because date 2025-09-09 doesn't match constant A
    ),
]


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_AT_LEAST_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_at_least_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_at_least_one_element_with(params)
    pd.testing.assert_series_equal(result, expected_result)


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_EXACTLY_ONE_ELEMENT_TEST_DATA,
)
def test_sql_shares_exactly_one_element_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_exactly_one_element_with(params)
    pd.testing.assert_series_equal(result, expected_result)


@pytest.mark.parametrize(
    "data,params,expected_result",
    SHARES_NO_ELEMENTS_TEST_DATA,
)
def test_sql_shares_no_elements_with(data, params, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.shares_no_elements_with(params)
    pd.testing.assert_series_equal(result, expected_result)


SHARES_EDGE_CASES = [
    # Operation variable edge cases only
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$constant"},
        [True],  # at_least_one: A is shared with itself
        [True],  # exactly_one: exactly one element A is shared
        [False],  # no_elements: A is shared, so not no elements
    ),
    (
        {"dummy": ["value"]},
        {"target": "$constant", "comparator": "$date"},
        [False],  # at_least_one: no shared elements (A vs 2025-09-09)
        [False],  # exactly_one: no shared elements
        [True],  # no_elements: no shared elements
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$constant"},
        [True],  # at_least_one: list [A,B] contains A which matches constant A
        [True],  # exactly_one: exactly one element (A) is shared
        [False],  # no_elements: A is shared
    ),
    (
        {"dummy": ["value"]},
        {"target": "$list", "comparator": "$date"},
        [False],  # at_least_one: list [A,B] doesn't contain date 2025-09-09
        [False],  # exactly_one: no shared elements
        [True],  # no_elements: no shared elements
    ),
]


@pytest.mark.parametrize(
    "data,params,expected_at_least_one,expected_exactly_one,expected_no_elements",
    SHARES_EDGE_CASES,
)
def test_sql_shares_elements_edge_cases(
    data, params, expected_at_least_one, expected_exactly_one, expected_no_elements
):
    sql_ops = create_sql_operators(data)

    result_at_least_one = sql_ops.shares_at_least_one_element_with(params)
    result_exactly_one = sql_ops.shares_exactly_one_element_with(params)
    result_no_elements = sql_ops.shares_no_elements_with(params)

    # Convert expected lists to Series for proper comparison
    expected_at_least_one_series = pd.Series(expected_at_least_one, dtype=bool)
    expected_exactly_one_series = pd.Series(expected_exactly_one, dtype=bool)
    expected_no_elements_series = pd.Series(expected_no_elements, dtype=bool)

    pd.testing.assert_series_equal(result_at_least_one, expected_at_least_one_series)
    pd.testing.assert_series_equal(result_exactly_one, expected_exactly_one_series)
    pd.testing.assert_series_equal(result_no_elements, expected_no_elements_series)
