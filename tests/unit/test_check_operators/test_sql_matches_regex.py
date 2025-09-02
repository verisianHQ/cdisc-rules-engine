import pandas as pd
import pytest
from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


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
    ],
)
def test_sql_matches_regex(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.matches_regex({"target": "target", "comparator": comparator})
    assert result.equals(pd.Series(expected_result))


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
    ],
)
def test_sql_not_matches_regex(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.not_matches_regex({"target": "target", "comparator": comparator})
    assert result.equals(pd.Series(expected_result))