"""Helper functions for SQL operator tests."""

import pandas as pd
from typing import Optional

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

TEST_TABLE_NAME = "test_table"
_LAST_DATA_SERVICE: Optional[PostgresQLDataService] = None
_LAST_DATASET_ID: Optional[str] = None


def set_sql_test_context(data_service: PostgresQLDataService, dataset_id: str):
    """Set SQL context for assertions when tests create operators without create_sql_operators."""
    global _LAST_DATA_SERVICE, _LAST_DATASET_ID
    _LAST_DATA_SERVICE = data_service
    _LAST_DATASET_ID = dataset_id


def create_sql_operators(
    column_data: dict,
    extra_operation_variables: dict = {},
    extra_config: dict = {},
    dataset_name: Optional[str] = None,
) -> PostgresQLOperators:
    """Create PostgresQLOperators instance with test data.
    It will preload some operation variables which can be used in tests.

    Args:
        column_data: Dictionary containing column names and their data
        extra_operation_variables: Optional additional custom dictionary of operation variables
        extra_config: Optional additional configuration for the operators
        dataset_name: Optional dataset name for testing dataset_name column operations

    Returns:
        PostgresQLOperators instance configured for testing
    """
    standards_context = DefaultStandardsContext()
    data_service = PostgresQLDataService.instance()

    table_name = dataset_name or TEST_TABLE_NAME
    PostgresQLDataService.add_test_dataset(
        data_service, table_name=table_name, column_data=column_data, standards_context=standards_context
    )

    global _LAST_DATA_SERVICE, _LAST_DATASET_ID
    _LAST_DATA_SERVICE = data_service
    _LAST_DATASET_ID = table_name

    config = {**extra_config, "dataset_id": table_name, "data_service": data_service}

    config["operation_variables"] = {**extra_operation_variables}
    config["operation_variables"]["$constant"] = SqlOperationResult(query="SELECT 'A'", type="constant", subtype="Char")
    config["operation_variables"]["$number"] = SqlOperationResult(query="SELECT 1.0", type="constant", subtype="Num")
    config["operation_variables"]["$date"] = SqlOperationResult(
        query="SELECT '2025-09-09'", type="constant", subtype="Char"
    )
    config["operation_variables"]["$list"] = SqlOperationResult(
        query="SELECT value FROM (VALUES ('A'), ('B')) as t(value)", type="collection", subtype="Char"
    )
    config["operation_variables"]["$empty_date"] = SqlOperationResult(
        query="SELECT NULL", type="constant", subtype="Date"
    )

    config["dataset_metadata"] = data_service.get_dataset_metadata(
        table_name,
    )

    return PostgresQLOperators(config)


def _resolve_active_context():
    """Get the latest SQL test context used to evaluate SQL condition strings."""
    data_service = _LAST_DATA_SERVICE
    dataset_id = _LAST_DATASET_ID

    if data_service and not dataset_id:
        uploaded = data_service.get_uploaded_dataset_ids()
        dataset_id = uploaded[-1] if uploaded else None

    if not data_service or not dataset_id:
        raise AssertionError("No active SQL test dataset found to evaluate SQL operator result.")

    return data_service, dataset_id


def _evaluate_sql_condition_to_series(sql_condition: str) -> pd.Series:
    """Evaluate a SQL boolean condition for all rows in the active test dataset."""
    data_service, preferred_dataset_id = _resolve_active_context()

    candidates = []
    if preferred_dataset_id:
        candidates.append(preferred_dataset_id)
    candidates.extend([ds for ds in data_service.get_uploaded_dataset_ids() if ds not in candidates])
    candidates = list(reversed(candidates))

    errors = []
    for dataset_id in candidates:
        table_hash = data_service.pgi.schema.get_table_hash(dataset_id)
        if not table_hash:
            continue

        query = f"""
            SELECT COALESCE(({sql_condition}), FALSE) AS result
            FROM {table_hash} co
            ORDER BY co.id
        """
        try:
            data_service.pgi.execute_sql(query)
            rows = data_service.pgi.fetch_all()
            values = []
            for row in rows:
                if row is None:
                    values.append(False)
                else:
                    value = row.get("result") if isinstance(row, dict) else False
                    values.append(bool(value) if value is not None else False)
            return pd.Series(values, dtype=bool)
        except Exception as exc:
            errors.append(f"{dataset_id}: {exc}")

    message = "Unable to evaluate SQL condition against any active dataset."
    if errors:
        message += " Tried datasets: " + " | ".join(errors)
    raise AssertionError(message)


def assert_series_equals(actual, expected):
    """Assert that pandas Series equals expected values.

    Args:
        actual: The actual pandas Series result or SQL condition string
        expected: Expected list of values or pandas Series
    """
    if isinstance(actual, str):
        actual = _evaluate_sql_condition_to_series(actual)

    if not isinstance(actual, pd.Series):
        raise AssertionError(f"Expected actual to be pandas Series or SQL string, got {type(actual)}")

    if isinstance(expected, pd.Series):
        expected_series = expected
    else:
        expected_series = pd.Series(expected, dtype=bool)

    if expected_series.dtype != actual.dtype:
        expected_series = expected_series.astype(actual.dtype)

    if not actual.equals(expected_series):
        failing_rows = []
        for i in range(min(len(actual), len(expected_series))):
            if actual.iloc[i] != expected_series.iloc[i]:
                failing_rows.append(f"Row {i}: expected {expected_series.iloc[i]}, got {actual.iloc[i]}")

        if len(actual) != len(expected_series):
            failing_rows.append(f"Length mismatch: expected {len(expected_series)}, got {len(actual)}")

        error_msg = "\nAssertion failed\n"
        error_msg += "Failing rows:\n"
        for row_info in failing_rows:
            error_msg += f"  {row_info}\n"
        error_msg += f"Expected: {expected_series.tolist()}\n"
        error_msg += f"Actual:   {actual.tolist()}"

        assert False, error_msg

    assert actual.equals(expected_series)
