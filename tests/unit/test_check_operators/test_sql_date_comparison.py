import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-25", "2023-03-10"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-05"]},
            "2023-02-20",
            True,
            [False, True, False],
        ),
    ],
)
def test_sql_date_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_equal_to({"target": "target", "comparator": comparator, "value_is_literal": value_is_literal})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-25", "2023-03-10"]},
            "VAR2",
            False,
            [False, True, False],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-05"]},
            "2023-02-20",
            True,
            [True, False, True],
        ),
    ],
)
def test_sql_date_not_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_not_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "2023-02-01",
            True,
            [True, False, False],
        ),
    ],
)
def test_sql_date_less_than(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_less_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-20", "2023-03-15"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "2023-02-20",
            True,
            [True, True, False],
        ),
    ],
)
def test_sql_date_less_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "2023-02-01",
            True,
            [False, True, True],
        ),
    ],
)
def test_sql_date_greater_than(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_greater_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-20", "2023-03-05"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "2023-02-20",
            True,
            [False, True, True],
        ),
    ],
)
def test_sql_date_greater_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))
