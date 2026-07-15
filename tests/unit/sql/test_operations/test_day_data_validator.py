import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import assert_operation_constant


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                "EXSTDTC": [
                    "1997-07-19T19:20:30",
                    "1997-08-16T19:20:30",
                    "1997-07-16T19:20",
                    "2022-05-20T13:44",
                    "2022-05-20T13:44",
                    None,
                    "2022-05-19T13:44",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4, 5, 6, 7],
                "RFSTDTC": [
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20",
                    "2022-05-08T13:44",
                    "TEST",
                    "2022-05-20T13:44",
                    "2022-05-20T13:44",
                ],
            },
            [4, 32, 1, 13, None, None, -1],
        ),
        (
            {
                "USUBJID": [1, 2, 3],
                "EXSTDTC": [
                    "2023-01-01T12:00:00",
                    "2023-01-02T00:00:00",
                    "2022-12-31T23:59:59",
                ],
            },
            {
                "USUBJID": [1, 2, 3],
                "RFSTDTC": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T12:00:00",
                    "2023-01-01T00:00:00",
                ],
            },
            [1, 2, -1],
        ),
    ],
)
def test_sql_dy_calculation(current_data, dm_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert result.type == "window"
    data_service.pgi.execute_sql(result.query)
    query_results = data_service.pgi.fetch_all()
    query_results.sort(key=lambda x: x["id"])
    actual_values = [row["value"] for row in query_results]
    assert actual_values == expected


@pytest.mark.parametrize(
    "current_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3],
                "EXSTDTC": [
                    "2023-01-01T12:00:00",
                    "2023-01-02T00:00:00",
                    "2022-12-31T23:59:59",
                ],
            },
            0,
        ),
    ],
)
def test_sql_dy_no_dm_domain(current_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 999],
                "EXSTDTC": [
                    "1997-07-19T19:20:30",
                    "1997-08-16T19:20:30",
                    "1997-07-16T19:20",
                ],
            },
            {
                "USUBJID": [1, 2],
                "RFSTDTC": [
                    "1997-07-16T19:20:30",
                    "1997-07-16T19:20:30",
                ],
            },
            [4, 32, None],
        ),
    ],
)
def test_sql_dy_missing_usubjid(current_data, dm_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert result.type == "column"
    data_service.pgi.execute_sql(result.query)
    query_results = data_service.pgi.fetch_all()
    query_results.sort(key=lambda x: x["id"])
    actual_values = [row["value"] for row in query_results]
    assert actual_values == expected


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
            {
                "USUBJID": [1, 2, 3, 4],
                "EXSTDTC": [
                    "invalid-date",
                    "2023-1-1",
                    "",
                    "2023-01-01",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4],
                "RFSTDTC": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                    "2023-01-01T00:00:00",
                ],
            },
            [None, None, None, 1],
        ),
    ],
)
def test_sql_dy_invalid_dates(current_data, dm_data, expected, sdtm_standards_context):
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=current_data, standards_context=sdtm_standards_context
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)
    result = operation.execute()

    assert result.type == "column"
    data_service.pgi.execute_sql(result.query)
    query_results = data_service.pgi.fetch_all()
    query_results.sort(key=lambda x: x["id"])
    actual_values = [row["value"] for row in query_results]
    assert actual_values == expected
