from unittest.mock import patch

from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.split import SqlSplitOperation
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_split_constant_operation(sdtm_standards_context):
    target_name = "test_constant"
    prev_op = SqlOperationResult(query="'A, B, C'", type="constant", subtype="Char")

    params = SqlOperationParams(
        domain="AE",
        target=target_name,
        standards_context=sdtm_standards_context,
        previous_operations={target_name: prev_op},
        delimiter=",",
    )

    data_service = PostgresQLDataService.instance()
    operation = SqlSplitOperation(params=params, data_service=data_service)
    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"
    assert "SELECT TRIM(UNNEST(STRING_TO_ARRAY(('A, B, C')::text, ','))) AS value" in result.query

    data_service.pgi.execute_sql(result.query)
    query_result = data_service.pgi.fetch_all()

    assert query_result == [{"value": "A"}, {"value": "B"}, {"value": "C"}]


def test_split_collection_operation(sdtm_standards_context):
    target_name = "test_collection"
    collection_query = "SELECT 'X|Y' AS value UNION ALL SELECT 'Z' AS value"
    prev_op = SqlOperationResult(query=collection_query, type="collection", subtype="Char")

    params = SqlOperationParams(
        domain="AE",
        target=target_name,
        standards_context=sdtm_standards_context,
        previous_operations={target_name: prev_op},
        delimiter="|",
    )

    data_service = PostgresQLDataService.instance()
    operation = SqlSplitOperation(params=params, data_service=data_service)
    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"

    data_service.pgi.execute_sql(result.query)
    query_result = data_service.pgi.fetch_all()

    values = sorted([r["value"] for r in query_result])
    assert values == ["X", "Y", "Z"]


def test_split_dataset_column(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    table_name = "test_split_dataset_table"
    col_name = "test_col"
    data_service.pgi.execute_sql(f"CREATE TABLE IF NOT EXISTS {table_name} (id serial, {col_name} text)")
    data_service.pgi.execute_sql(f"INSERT INTO {table_name} ({col_name}) VALUES ('A, B'), ('C'), (NULL), ('')")

    with (
        patch.object(data_service.pgi.schema, "column_exists", return_value=True),
        patch.object(data_service.pgi.schema, "get_table_hash", return_value=table_name),
        patch.object(data_service.pgi.schema, "get_column_hash", return_value=col_name),
    ):

        params = SqlOperationParams(
            domain="AE",
            target=col_name,
            standards_context=sdtm_standards_context,
            previous_operations={},
            delimiter=",",
        )

        operation = SqlSplitOperation(params=params, data_service=data_service)
        result = operation.execute()

        data_service.pgi.execute_sql(result.query)
        query_result = data_service.pgi.fetch_all()

        values = sorted([r["value"] for r in query_result])
        assert values == ["A", "B", "C"]

    data_service.pgi.execute_sql(f"DROP TABLE {table_name}")
