import pytest
from .helpers import assert_series_equals
from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "domain, target, operator, data, result",
    [
        (
            "CM",
            "CMTRT",
            "is_null",
            {
                "CMTRT": [None, "", "null"],
            },
            [True, True, False],
        ),
        (
            "CM",
            "CMDOSE",
            "is_null",
            {
                "CMDOSE": [None, 0],
            },
            [True, False],
        ),
        (
            "CM",
            "CMTRT",
            "is_not_null",
            {
                "CMTRT": [None, "", "null"],
            },
            [False, False, True],
        ),
        (
            "CM",
            "CMDOSE",
            "is_not_null",
            {
                "CMDOSE": [None, 0],
            },
            [False, True],
        ),
    ],
)
def test_is_null(sdtm_standards_context, domain, target, operator, data, result):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = getattr(PostgresQLOperators(config), operator)({"target": target})
    assert_series_equals(op_result, result)
