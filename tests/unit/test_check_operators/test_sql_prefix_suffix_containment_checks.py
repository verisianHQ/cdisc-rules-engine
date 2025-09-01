import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

PREFIX_IS_CONTAINED_BY_TEST_DATA = [
    (
        {"var1": ["AETEST", "AETESTCD", "LBTEST"], "domain_col": ["AE", "XX", "RR"]},
        "var1",
        "domain_col",
        False,
        2,
        [True, True, False],
    ),
    (
        {"var2": ["AETEST", "AFTESTCD", "RRTEST"], "domain_col": ["AE", "XX", "RR"]},
        "var2",
        "domain_col",
        False,
        2,
        [True, False, True],
    ),
    (
        {"var1": ["AETEST", "AETESTCD", "LBTEST"]},
        "var1",
        ["AE", "LB"],
        True,
        2,
        [True, True, True],
    ),
    (
        {"var2": ["AETEST", "AFTESTCD", "RRTEST"]},
        "var2",
        ["AE", "RR"],
        True,
        2,
        [True, False, True],
    ),
]

SUFFIX_IS_CONTAINED_BY_TEST_DATA = [
    (
        {"var1": ["AETEST", "AETESTCD", "LBTEGG"], "suffix_col": ["ST", "CD", "LE"]},
        "var1",
        "suffix_col",
        False,
        2,
        [True, True, False],
    ),
    (
        {"var2": ["AETEST", "AFTESTCD", "RRTELE"], "suffix_col": ["ST", "CD", "LE"]},
        "var2",
        "suffix_col",
        False,
        2,
        [True, True, True],
    ),
    (
        {"var1": ["AETEST", "AETESTCD", "LBTEGG"]},
        "var1",
        ["ST", "CD", "GG"],
        True,
        2,
        [True, True, True],
    ),
    (
        {"var2": ["AETEST", "AFTESTCD", "RRTELE"]},
        "var2",
        ["ST", "CD"],
        True,
        2,
        [True, True, False],
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_contained_by(data, target, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.prefix_is_contained_by(
        {
            "target": target,
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_not_contained_by(data, target, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.prefix_is_not_contained_by(
        {
            "target": target,
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(~pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_contained_by(data, target, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.suffix_is_contained_by(
        {
            "target": target,
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_not_contained_by(data, target, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.suffix_is_not_contained_by(
        {
            "target": target,
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(~pd.Series(expected_result))


# Note: The following test cases cannot be implemented in SQL due to PostgreSQL serializer limitations.
# SQL databases don't support columns containing list values like the DataFrame implementation does.
# These would be equivalent to the DataFrame tests that use "list-per-row" comparisons:
#
# PREFIX_SUFFIX_LIST_PER_ROW_TEST_DATA = [
#     # This pattern works in DataFrame tests but fails in SQL:
#     (
#         {
#             "var1": ["AETEST", "AETESTCD", "LBTEST"],
#             "study_domains": [
#                 ["DM", "AE", "LB", "TV"],  # List per row - not supported in SQL
#                 ["DM", "AE", "LB", "TV"],
#                 ["DM", "AE", "LB", "TV"],
#             ]
#         },
#         "study_domains",  # Comparator column containing lists
#         False,            # value_is_literal=False (column comparison)
#         2,                # prefix_length
#         [True, True, True]  # Expected: each row's prefix checked against that row's list
#     ),
#     (
#         {
#             "var1": ["AETEST", "AETESTCD", "LBTEGG"],
#             "study_domains": [
#                 ["ST", "CD", "GG", "TV"],
#                 ["ST", "CD", "GG", "TV"],
#                 ["ST", "CD", "GG", "TV"],
#             ]
#         },
#         "study_domains",
#         False,
#         2,                # suffix_length
#         [True, True, True]  # Expected: each row's suffix checked against that row's list
#     )
# ]
#
# Error when attempted: "ValueError: Unsupported type: <class 'list'>"
# SQL workaround: Use literal list comparators or simple column-to-column comparisons instead.
