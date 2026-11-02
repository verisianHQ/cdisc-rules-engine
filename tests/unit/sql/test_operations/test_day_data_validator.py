import pytest

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlOperationError
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import assert_operation_constant


def _dy_operation(data_service, table: str, standards_context):
    params = SqlOperationParams(domain="EX", target="EXSTDTC", table=table, standards_context=standards_context)
    return SqlOperationsFactory.get_service("dy", params, data_service)


def _study_days(current_data, dm_data, standards_context) -> list:
    """Study day of every row of EX merged with DM (INNER, as the rules merge DM), each row using its own columns."""
    data_service = PostgresQLDataService.instance()
    for name, data in (("DM", dm_data), ("EX", current_data)):
        PostgresQLDataService.add_test_dataset(
            data_service, table_name=name, column_data=data, standards_context=standards_context
        )
    schema = data_service.pgi.schema
    merged = SqlJoinMerge.perform_join(
        pgi=data_service.pgi,
        left=schema.get_table("EX"),
        right=schema.get_table("DM"),
        pivot_left=["USUBJID"],
        pivot_right=["USUBJID"],
        type="INNER",
    )
    result = _dy_operation(data_service, merged.name, standards_context).execute()

    query = result.query
    for placeholder, column in result.params.items():
        query = query.replace(placeholder, f"t.{merged.get_column_hash(column)}")
    data_service.pgi.execute_sql(f"SELECT ({query}) AS value FROM {merged.hash} t ORDER BY t.id")
    return [row["value"] for row in data_service.pgi.fetch_all()]


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        pytest.param(
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
            id="calculation",
        ),
        pytest.param(
            {
                "USUBJID": [1, 2, 3, 4],
                "EXSTDTC": [
                    "2023-01-01T12:00:00",
                    "2023-01-02T00:00:00",
                    "2022-12-31T23:59:59",
                    "2022-12-31T23:59:59",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4],
                "RFSTDTC": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T12:00:00",
                    "2023-01-01T00:00:00",
                    "2022-12-31",
                ],
            },
            [1, 2, -1, 1],
            id="date_portion_only",
        ),
        pytest.param(
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
            [4, 32],
            id="subject_missing_from_dm_last",
        ),
        pytest.param(
            {"USUBJID": [1, 2, 3], "EXSTDTC": ["2023-01-05", "2023-01-10", "2023-02-20"]},
            {"USUBJID": [2, 3], "RFSTDTC": ["2023-01-01", "2023-02-01"]},
            [10, 20],
            id="subject_missing_from_dm_first",
        ),
        pytest.param(
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
            id="invalid_dates",
        ),
        pytest.param(
            {
                "USUBJID": [1, 2, 3, 4, 5, 6],
                "EXSTDTC": [
                    "2023-01-10",
                    "2023-01",
                    "2023",
                    "2023-01-10T12:00",
                    "2023-01-9",
                    "2023-01-10",
                ],
            },
            {
                "USUBJID": [1, 2, 3, 4, 5, 6],
                "RFSTDTC": [
                    "2023-01-01",
                    "2023-01-05",
                    "2023-06-05",
                    "2023-01-01T00:00",
                    "2023-01-01",
                    "2023-01-9",
                ],
            },
            [10, 1, 1, 10, None, None],
            id="truncates_to_lower_precision",
        ),
    ],
)
def test_sql_dy(current_data, dm_data, expected, sdtm_standards_context):
    assert _study_days(current_data, dm_data, sdtm_standards_context) == expected


def test_sql_dy_dataset_without_target(sdtm_standards_context):
    """A dataset without the target isn't checked, whether or not it has RFSTDTC."""
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data={"USUBJID": [1]}, standards_context=sdtm_standards_context
    )

    operation = _dy_operation(data_service, "EX", sdtm_standards_context)
    assert_operation_constant(operation, operation.execute(), 0)


def test_sql_dy_without_dm_merge(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="EX",
        column_data={"USUBJID": [1], "EXSTDTC": ["2023-01-01"]},
        standards_context=sdtm_standards_context,
    )

    with pytest.raises(SqlOperationError, match="must merge DM"):
        _dy_operation(data_service, "EX", sdtm_standards_context).execute()
