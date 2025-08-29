import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["Ctt", "Btt", "A"]},
            ["Ctt", "B", "A"],
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["C", "Z", "A"],
            [True, False, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "D"]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            "B",
            [False, True, False],
        ),
        # Note: Doesn't seem like there is a way to test this using SQL
        # (
        #     {"target": [1, 2, 3], "VAR2": [[1, 2], [3], [3]]},
        #     "VAR2",
        #     [True, False, True],
        # ),
    ],
)
def test_is_contained_by(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    # Determine if comparator is a literal list
    value_is_literal = isinstance(comparator, list)

    result = sql_ops.is_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        # Test with list of literal values
        (
            {"target": ["Ctt", "Btt", "A"]},
            ["Ctt", "B", "A"],
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            ["C", "Z", "A"],
            [False, True, False],
        ),
        # Test with column reference
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "D"]},
            "VAR2",
            [False, False, True],
        ),
        # Test with single literal value (3rd branch)
        (
            {"target": ["A", "B", "C"]},
            "B",
            [True, False, True],
        ),
    ],
)
def test_is_not_contained_by(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    # Determine if comparator is a literal list
    value_is_literal = isinstance(comparator, list)

    result = sql_ops.is_not_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))
