import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


def test_invalid_table_name():
    """Test that an exception is raised when an invalid table name is used."""
    data_service = PostgresQLDataService.test_instance()
    with pytest.raises(Exception):
        PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="SELECT", column_data={"key": [1]})


def test_uneven_columns():
    """Test that an exception is raised when the test data has columns with different lengths."""
    data_service = PostgresQLDataService.test_instance()
    with pytest.raises(Exception):
        PostgresQLDataService.add_test_dataset(
            data_service.pgi, table_name="test", column_data={"key": [1], "value": [1, 2]}
        )


def test_invalid_column_name():
    """Test that an exception is raised when the test data has a column with an invalid name."""
    data_service = PostgresQLDataService.test_instance()
    with pytest.raises(Exception):
        PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="test", column_data={"select": [1]})


def test_data_contains_id():
    """
    Test that an exception is raised when the test data has an 'id' column, as this is used as a row id by postgres.
    """
    data_service = PostgresQLDataService.test_instance()
    with pytest.raises(Exception):
        PostgresQLDataService.add_test_dataset(data_service.pgi, table_name="test", column_data={"id": [1]})
