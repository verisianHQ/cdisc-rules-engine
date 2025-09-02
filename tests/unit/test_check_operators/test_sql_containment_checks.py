import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

CONTAINS_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"], "VAR2": ["Ctt", "btt", "lll"]},
        "VAR2",
        False,
        [True, False, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        "B",
        True,
        [False, True, False],
    ),
    (
        {"target": ["Ctt", "Btt", "A"], "VAR2": ["Ctt", "Btt", "A"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["Ctt", "Btt", "A"], "VAR2": ["X", "Y"]},
        "VAR2",
        False,
        [False, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        ["C", "Z", "A"],
        True,
        [True, False, True],
    ),
    # Note: Doesn't seem like there is a way to test this using SQL
    # (
    #     {"target": [["A", "B", "C"], ["A", "B", "L"], ["L", "Q", "R"]]},
    #     "L",
    #     True,
    #     [False, True, True],
    # ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_TEST_DATA,
)
def test_sql_contains(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.contains(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_TEST_DATA,
)
def test_sql_does_not_contain(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.does_not_contain(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert result.equals(~pd.Series(expected_result))


CONTAINED_BY_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"]},
        ["Ctt", "B", "A"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        ["C", "Z", "A"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "D"]},
        "VAR2",
        False,
        [True, True, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        "B",
        True,
        [False, True, False],
    ),
    # Note: Doesn't seem like there is a way to test this using SQL
    # (
    #     {"target": [1, 2, 3], "VAR2": [[1, 2], [3], [3]]},
    #     "VAR2",
    #     [True, False, True],
    # ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_TEST_DATA,
)
def test_is_contained_by(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.is_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_TEST_DATA,
)
def test_is_not_contained_by(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.is_not_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(~pd.Series(expected_result))


CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"]},
        ["ctt", "b", "a"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        ["c", "z", "a"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        "b",
        True,
        [False, True, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA,
)
def test_is_contained_by_case_insensitive(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.is_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA,
)
def test_is_not_contained_by_case_insensitive(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.is_not_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(~pd.Series(expected_result))
