import pytest
from unittest.mock import patch, MagicMock
import json

from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory


@pytest.mark.parametrize(
    "mock_ig_variables, key_name, key_value, expected",
    [
        # Test filtering by role="Timing"
        (
            [("VISITNUM", "17"), ("VISIT", "18"), ("TIMING_VAR", "33")],
            "role",
            "Timing",
            ["VISITNUM", "VISIT", "TIMING_VAR"],
        ),
        # Test filtering by role="Identifier"
        ([("STUDYID", "1"), ("DOMAIN", "2"), ("USUBJID", "3")], "role", "Identifier", ["STUDYID", "DOMAIN", "USUBJID"]),
        # Test with wildcard replacement
        (
            [
                ("--TERM", "1"),
            ],
            "role",
            "Topic",
            ["AETERM"],  # --TERM becomes AETERM for AE domain
        ),
        # Test no matches
        ([], "role", "NonExistentRole", []),
    ],
)
@patch("cdisc_rules_engine.data_service.postgresql_data_service.PostgresQLDataService.instance")
def test_get_model_filtered_variables(mock_data_service_instance, mock_ig_variables, key_name, key_value, expected):
    """Test get_model_filtered_variables operation with different filter criteria"""

    # Mock data service and PostgreSQL interface
    mock_data_service = MagicMock()
    mock_pgi = MagicMock()
    mock_data_service.pgi = mock_pgi
    mock_data_service_instance.return_value = mock_data_service

    # Mock database queries
    mock_pgi.execute_sql.return_value = len(mock_ig_variables)
    mock_pgi.fetch_all.return_value = mock_ig_variables

    # Set up parameters
    params = SqlOperationParams(
        domain="ae", target=None, standard="sdtmig", standard_version="3-4", key_name=key_name, key_value=key_value
    )

    # Get operation
    operation = SqlOperationsFactory.get_service("get_model_filtered_variables", params, mock_data_service)

    # Execute operation
    result = operation.execute()

    # Verify result
    assert result is not None
    expected_json = json.dumps(expected)
    # The operation returns a constant result with the JSON string
    assert expected_json in result.query
    assert result.type == "constant"


@patch("cdisc_rules_engine.data_service.postgresql_data_service.PostgresQLDataService.instance")
def test_get_model_filtered_variables_with_empty_dataset(mock_data_service_instance):
    """Test get_model_filtered_variables with empty result"""
    mock_ig_variables = [("AETERM", "1"), ("AEDECOD", "2")]

    # Mock data service and PostgreSQL interface
    mock_data_service = MagicMock()
    mock_pgi = MagicMock()
    mock_data_service.pgi = mock_pgi
    mock_data_service_instance.return_value = mock_data_service

    # Mock database queries
    mock_pgi.execute_sql.return_value = len(mock_ig_variables)
    mock_pgi.fetch_all.return_value = mock_ig_variables

    # Set up parameters
    params = SqlOperationParams(
        domain="ae", target=None, standard="sdtmig", standard_version="3-4", key_name="role", key_value="Topic"
    )

    # Get operation
    operation = SqlOperationsFactory.get_service("get_model_filtered_variables", params, mock_data_service)

    # Execute operation
    result = operation.execute()

    # Verify result
    assert result is not None
    expected = ["AETERM", "AEDECOD"]
    expected_json = json.dumps(expected)
    # The operation returns a constant result with the JSON string
    assert expected_json in result.query
    assert result.type == "constant"


@patch("cdisc_rules_engine.data_service.postgresql_data_service.PostgresQLDataService.instance")
def test_get_model_filtered_variables_with_exception_handling(mock_data_service_instance):
    """Test get_model_filtered_variables when database operation fails"""

    # Mock data service and PostgreSQL interface
    mock_data_service = MagicMock()
    mock_pgi = MagicMock()
    mock_data_service.pgi = mock_pgi
    mock_data_service_instance.return_value = mock_data_service

    # Mock database query to raise exception
    mock_pgi.execute_sql.side_effect = Exception("Database connection failed")

    # Set up parameters
    params = SqlOperationParams(
        domain="ae", target=None, standard="sdtmig", standard_version="3-4", key_name="role", key_value="Topic"
    )

    # Get operation
    operation = SqlOperationsFactory.get_service("get_model_filtered_variables", params, mock_data_service)

    # Execute operation - should return empty result on exception
    result = operation.execute()

    # Verify result
    assert result is not None
    expected_json = json.dumps([])
    # The operation returns a constant result with the JSON string
    assert expected_json in result.query
    assert result.type == "constant"
