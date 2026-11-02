import pytest

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import assert_operation_constant, assert_operation_parameterized_constant


def _row_cases(expected, current_data, target):
    """Turn cases keyed by row number into the row's own $1 (target date) and $2 (USUBJID) values."""
    return [
        {
            "params": {"$1": current_data[target][case["row"] - 1], "$2": current_data["USUBJID"][case["row"] - 1]},
            "value": case["value"],
        }
        for case in expected
    ]


def _dy_per_row(data_service, table, result) -> list:
    """Evaluate the op for every row of table, substituting each row's own columns as the check does."""
    query = result.query
    for placeholder, column in result.params.items():
        query = query.replace(placeholder, f"t.{table.get_column_hash(column)}")
    data_service.pgi.execute_sql(f"SELECT ({query}) AS value FROM {table.hash} t ORDER BY t.id")
    return [row["value"] for row in data_service.pgi.fetch_all()]


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
            [
                {"row": 1, "value": [4]},
                {"row": 2, "value": [32]},
                {"row": 3, "value": [1]},
                {"row": 4, "value": [13]},
                {"row": 5, "value": [None]},
                {"row": 6, "value": [None]},
                {"row": 7, "value": [-1]},
            ],
        ),
        (
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
            [
                {"row": 1, "value": [1]},
                {"row": 2, "value": [2]},
                {"row": 3, "value": [-1]},
                {"row": 4, "value": [1]},
            ],
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

    assert_operation_parameterized_constant(operation, result, _row_cases(expected, current_data, "EXSTDTC"))


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
            [
                {"row": 1, "value": [4]},
                {"row": 2, "value": [32]},
                {"row": 3, "value": [None]},
            ],
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

    assert_operation_parameterized_constant(operation, result, _row_cases(expected, current_data, "EXSTDTC"))


def test_sql_dy_dataset_without_usubjid(sdtm_standards_context):
    """Without USUBJID the dataset can't be joined to DM for RFSTDTC, so the rule is skipped."""
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="DM",
        column_data={"USUBJID": [1], "RFSTDTC": ["1997-07-16"]},
        standards_context=sdtm_standards_context,
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="TS",
        column_data={"TSPARMCD": ["SSTDTC"], "TSVAL": ["1997-07-19"]},
        standards_context=sdtm_standards_context,
    )

    params = SqlOperationParams(domain="TS", target="TSVAL", standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("dy", params, data_service)

    with pytest.raises(ColumnNotFoundError, match="Column 'USUBJID' not found in table 'ts'"):
        operation.execute()


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
            [
                {"row": 1, "value": [None]},
                {"row": 2, "value": [None]},
                {"row": 3, "value": [None]},
                {"row": 4, "value": [1]},
            ],
        ),
    ],
)
def test_sql_dy_invalid_dates(current_data, dm_data, expected, sdtm_standards_context):
    """Test DY calculation with various invalid date formats."""
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

    assert_operation_parameterized_constant(operation, result, _row_cases(expected, current_data, "EXSTDTC"))


@pytest.mark.parametrize(
    "current_data, dm_data, expected",
    [
        (
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
            [
                {"row": 1, "value": [10]},
                {"row": 2, "value": [1]},
                {"row": 3, "value": [1]},
                {"row": 4, "value": [10]},
                {"row": 5, "value": [None]},
                {"row": 6, "value": [None]},
            ],
        ),
    ],
)
def test_sql_dy_truncates_to_lower_precision(current_data, dm_data, expected, sdtm_standards_context):
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

    assert_operation_parameterized_constant(operation, result, _row_cases(expected, current_data, "EXSTDTC"))


def _add_dm_and_ex(data_service, standards_context, dm_data, ex_data):
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="DM", column_data=dm_data, standards_context=standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="EX", column_data=ex_data, standards_context=standards_context
    )
    return data_service.pgi.schema.get_table("EX"), data_service.pgi.schema.get_table("DM")


def test_sql_dy_reads_rfstdtc_from_rule_merged_row(sdtm_standards_context):
    """With the rule's DM merge (INNER, so subject 1 is dropped), each row uses its own RFSTDTC."""
    data_service = PostgresQLDataService.instance()
    ex, dm = _add_dm_and_ex(
        data_service,
        sdtm_standards_context,
        {"USUBJID": [2, 3], "RFSTDTC": ["2023-01-01", "2023-02-01"]},
        {"USUBJID": [1, 2, 3], "EXSTDTC": ["2023-01-05", "2023-01-10", "2023-02-20"]},
    )
    merged = SqlJoinMerge.perform_join(
        pgi=data_service.pgi, left=ex, right=dm, pivot_left=["USUBJID"], pivot_right=["USUBJID"], type="INNER"
    )

    params = SqlOperationParams(
        domain="EX", target="EXSTDTC", table=merged.name, standards_context=sdtm_standards_context
    )
    result = SqlOperationsFactory.get_service("dy", params, data_service).execute()

    assert result.params == {"$1": "EXSTDTC", "$2": "RFSTDTC"}
    assert _dy_per_row(data_service, merged, result) == [10, 20]


def test_sql_dy_looks_up_rfstdtc_in_dm_by_usubjid(sdtm_standards_context):
    """Without a DM merge, RFSTDTC comes from DM by the row's USUBJID: unmatched rows get no study day,
    a subject repeated in DM uses its first record, and later rows are unaffected."""
    data_service = PostgresQLDataService.instance()
    ex, _ = _add_dm_and_ex(
        data_service,
        sdtm_standards_context,
        {"USUBJID": ["S2", "S3", "S2"], "RFSTDTC": ["2023-01-01", "2023-02-01", "2020-06-01"]},
        {"USUBJID": ["S1", "S2", "", "S3"], "EXSTDTC": ["2023-01-05", "2023-01-10", "2023-01-10", "2023-02-20"]},
    )

    params = SqlOperationParams(domain="EX", target="EXSTDTC", standards_context=sdtm_standards_context)
    result = SqlOperationsFactory.get_service("dy", params, data_service).execute()

    assert result.params == {"$1": "EXSTDTC", "$2": "USUBJID"}
    assert _dy_per_row(data_service, ex, result) == [None, 10, None, 20]
