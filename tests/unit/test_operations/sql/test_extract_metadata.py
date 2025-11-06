from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from .helpers import assert_operation_constant
import pytest
from unittest.mock import patch
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)


class DummyDatasetMetadata:
    def __init__(self, filename=None, name=None, domain=None, size=None):
        self.filename = filename
        self.name = name
        self.domain = domain
        self.size = size


test_dataset_metadata = [
    DummyDatasetMetadata(
        filename="ae.xpt",
        name="AE",
        domain="AE",
    ),
    DummyDatasetMetadata(
        filename="supplb.xpt",
        name="SUPPLB",
        domain="SUPPQUAL",
    ),
]


@pytest.mark.parametrize(
    "mock_datasets, expected",
    [
        (
            test_dataset_metadata,
            "AE",
        ),
    ],
)
def test_dataset_name_extract_metadata(mock_datasets, expected):
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=standards_context,
    )

    params = SqlOperationParams(domain="AE", target="dataset_name", standards_context=standards_context)

    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        result = operation.execute()
        assert_operation_constant(operation, result, expected)


test_size_dataset_metadata = [
    DummyDatasetMetadata(filename="dm.xpt", name="DM", domain="DM", size="5GB"),
]


@pytest.mark.parametrize(
    "mock_datasets, expected",
    [
        (
            test_size_dataset_metadata,
            "5GB",
        ),
    ],
)
def test_size_extract_metadata(mock_datasets, expected):
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=standards_context,
    )

    params = SqlOperationParams(domain="AE", target="size", standards_context=standards_context)

    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        result = operation.execute()
        assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "mock_datasets",
    [
        (test_size_dataset_metadata, Exception),
    ],
)
def test_extract_metadata_exception_handling(mock_datasets):
    """Test extract_metadata errors when target metadata not present (eg weight)"""
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=standards_context,
    )

    params = SqlOperationParams(domain="AE", target="weight", standards_context=standards_context)

    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        with pytest.raises(Exception):
            operation.execute()
