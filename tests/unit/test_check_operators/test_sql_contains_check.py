import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["pie", "split", "cake"]},
            "VAR2",
            False,
            [True, True, False],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["apple", "banana", "cherry"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["orange", "split", "tart"]},
            "VAR2",
            False,
            [False, True, True],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"]},
            "pie",
            True,
            [True, False, False],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"]},
            "split",
            True,
            [False, True, False],
        ),
    ],
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
    [
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["pie", "split", "cake"]},
            "VAR2",
            False,
            [False, False, True],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["apple", "banana", "cherry"]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"], "VAR2": ["orange", "split", "tart"]},
            "VAR2",
            False,
            [True, False, False],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"]},
            "pie",
            True,
            [False, True, True],
        ),
        (
            {"target": ["apple pie", "banana split", "cherry tart"]},
            "split",
            True,
            [True, False, True],
        ),
    ],
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
    assert result.equals(pd.Series(expected_result))
