import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["Ctt", "btt", "lll"]},
            "VAR2",
            False,
            [True, False, False],
        ),
        (
            {"target": ["Ctt", "Btt", "A"]},
            "A",
            True,
            [False, False, True],
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["Ctt", "Btt", "A"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["X", "Y", "Z"]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["A", "1", "2"]},
            1,
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
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "btt", "lll"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["Ctt", "Btt", "A"]},
            "A",
            True,
            [True, True, False],
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["Ctt", "Btt", "A"]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["Ctt", "Btt", "A"], "VAR2": ["X", "Y", "Z"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["A", "1", "2"]},
            1,
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
