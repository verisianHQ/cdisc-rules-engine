import pytest

from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult

from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)

TEST_OPERATIONS = {
    "$op_name": SqlOperationResult(
        query="SELECT 'A' AS value UNION SELECT 'B' AS value", type="collection", subtype="Char"
    ),
    "$op_subtract": SqlOperationResult(
        query="SELECT 'B' AS value UNION SELECT 'C' AS value", type="collection", subtype="Char"
    ),
}


@pytest.mark.parametrize(
    "data, params, expected",
    [
        (
            {"col_name": ["A", "B", "C", "D"], "col_subtract": ["C", "D", "E", "F"]},
            {"name": "col_name", "subtract": "col_subtract"},
            ["C", "D"],
        ),
        (
            {"col_name": [1, 2, 3, 4], "col_subtract": [3, 4, 5, 6]},
            {"name": "col_name", "subtract": "col_subtract"},
            [3, 4],  # or ["3", "4"] depending on how the pg driver interprets the type in testing
        ),
        # Case 3: When 'name' and 'subtract' have no overlapping values (empty collection)
        (
            {"col_name": ["X", "Y", "Z"], "col_subtract": [None, None, "W"]},
            {"name": "col_name", "subtract": "col_subtract"},
            [],
        ),
        # Case 4: When the 'name' column is empty (should return empty collection)
        ({"col_name": [None, None], "col_subtract": ["A", "B"]}, {"name": "col_name", "subtract": "col_subtract"}, []),
        (
            {"dummy_col": ["1"]},  # table needs to be populated for setup_sql_operations to work
            {"name": "$op_name", "subtract": "$op_subtract"},
            ["B"],
        ),
    ],
)
def test_intersect_operation(data, params, expected):
    """
    Tests the INTERSECT operation.
    """
    operation = setup_sql_operations(
        operation="intersect",
        target=None,
        column_data=data,
        extra_config=params,
        extra_operation_variables=TEST_OPERATIONS,
    )

    result = operation.execute()
    assert_operation_collection(operation, result, expected, unsorted=True)


def test_intersect_operation_preserves_name_order():
    """
    The result should follow col_name's original row order (restricted to the overlap
    with col_subtract), not be reordered by a set-intersection dedup step.
    """
    operation = setup_sql_operations(
        operation="intersect",
        target=None,
        column_data={"col_name": ["D", "B", "A", "C", "E"], "col_subtract": ["A", "C", "E", "A", "C"]},
        extra_config={"name": "col_name", "subtract": "col_subtract"},
    )

    result = operation.execute()
    assert_operation_collection(operation, result, ["A", "C", "E"], unsorted=False)


def test_intersect_operation_deduplicates():
    """Duplicate values in 'name' collapse to their first occurrence."""
    operation = setup_sql_operations(
        operation="intersect",
        target=None,
        column_data={"col_name": ["A", "B", "A", "C"], "col_subtract": ["A", "C", "A", "C"]},
        extra_config={"name": "col_name", "subtract": "col_subtract"},
    )

    result = operation.execute()
    assert_operation_collection(operation, result, ["A", "C"], unsorted=False)
