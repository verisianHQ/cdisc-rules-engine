from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from .helpers import assert_operation_constant
import pytest
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)


class DummyStandardsContext(DefaultStandardsContext):
    def get_domain_metadata(self, domain):
        if domain == "AE":
            return {"filename": "ae.xpt", "name": "AE", "domain": "AE", "size": "5GB"}
        else:
            return {}


def test_dataset_name_extract_metadata():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="dataset_name", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "AE")


def test_size_extract_metadata():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="size", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "5GB")


def test_extract_metadata_exception_handling():
    """Test extract_metadata errors when target metadata not present (eg weight)"""
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="weight", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    with pytest.raises(Exception):
        operation.execute()


def _load_supplbch(data_service, standards_context):
    table = PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="supplbch",
        column_data={"RDOMAIN": ["LB", "DM"]},
        standards_context=standards_context,
    )
    return table, data_service.get_dataset_metadata("supplbch")


def test_dataset_name_extract_metadata_uses_validated_dataset():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    table, dataset_metadata = _load_supplbch(data_service, standards_context)
    params = SqlOperationParams(
        domain="SUPPLB",
        target="dataset_name",
        standards_context=standards_context,
        table="supplbch",
        dataset_metadata=dataset_metadata,
    )
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()

    assert result.type == "constant"
    query = result.query
    for placeholder, column in result.params.items():
        query = query.replace(placeholder, data_service.pgi.schema.get_column_hash("supplbch", column))
    data_service.pgi.execute_sql(f"SELECT ({query}) AS value FROM {table.hash} ORDER BY id")
    assert [row["value"] for row in data_service.pgi.fetch_all()] == ["SUPPLBCH", "SUPPLBCH"]


def test_dataset_name_extract_metadata_without_table_uses_dataset_metadata():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    _, dataset_metadata = _load_supplbch(data_service, standards_context)
    params = SqlOperationParams(
        domain="SUPPLB",
        target="dataset_name",
        standards_context=standards_context,
        dataset_metadata=dataset_metadata,
    )
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "SUPPLBCH")


@pytest.mark.parametrize("file_size", [6_000_000_000, None])
def test_dataset_size_extract_metadata_uses_validated_dataset(file_size):
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    _, dataset_metadata = _load_supplbch(data_service, standards_context)
    dataset_metadata.file_size = file_size
    params = SqlOperationParams(
        domain="SUPPLB",
        target="dataset_size",
        standards_context=standards_context,
        dataset_metadata=dataset_metadata,
    )
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert result.subtype == "Num"
    assert_operation_constant(operation, result, file_size)
