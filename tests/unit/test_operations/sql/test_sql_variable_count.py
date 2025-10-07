from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import (
    assert_operation_constant,
)


def test_variable_count_with_wildcard():
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data={
            "STUDYID": ["STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "SUBJ2"],
            "LBTESTCD": ["ALT", "AST"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="VS",
        column_data={
            "STUDYID": ["STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "SUBJ2"],
            "VSTESTCD": ["SYSBP", "DIABP"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="EG",
        column_data={
            "STUDYID": ["STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "SUBJ2"],
            "EGTESTCD": ["QT", "HR"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="DM",
        column_data={
            "STUDYID": ["STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "SUBJ2"],
            "AGE": [25, 30],
        },
    )

    params = SqlOperationParams(
        domain="LB",
        target="--TESTCD",
        standard="",
        standard_version="",
    )
    operation = SqlOperationsFactory.get_service("variable_count", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, 3)


def test_variable_count_no_matches():
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data={
            "STUDYID": ["STUDY1"],
            "USUBJID": ["SUBJ1"],
            "LBORRES": ["RESULT"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="VS",
        column_data={
            "STUDYID": ["STUDY1"],
            "USUBJID": ["SUBJ1"],
            "VSORRES": ["120"],
        },
    )

    params = SqlOperationParams(
        domain="LB",
        target="--NONEXISTENT",
        standard="",
        standard_version="",
    )
    operation = SqlOperationsFactory.get_service("variable_count", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, 0)


def test_variable_count_all_datasets():
    data_service = PostgresQLDataService.instance()

    for domain in ["LB", "VS", "EG", "DM"]:
        PostgresQLDataService.add_test_dataset(
            data_service,
            table_name=domain,
            column_data={
                "STUDYID": ["STUDY1", "STUDY1"],
                "USUBJID": ["SUBJ1", "SUBJ2"],
            },
        )

    params = SqlOperationParams(
        domain="LB",
        target="STUDYID",
        standard="",
        standard_version="",
    )
    operation = SqlOperationsFactory.get_service("variable_count", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, 4)


def test_variable_count_exact_match():
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data={
            "STUDYID": ["STUDY1"],
            "LBTESTCD": ["ALT"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="VS",
        column_data={
            "STUDYID": ["STUDY1"],
            "VSTESTCD": ["SYSBP"],
        },
    )

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="EG",
        column_data={
            "STUDYID": ["STUDY1"],
            "EGTESTCD": ["QT"],
        },
    )

    params = SqlOperationParams(
        domain="LB",
        target="LBTESTCD",
        standard="",
        standard_version="",
    )
    operation = SqlOperationsFactory.get_service("variable_count", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, 1)


def test_variable_count_single_dataset():
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="DM",
        column_data={
            "STUDYID": ["STUDY1", "STUDY1"],
            "USUBJID": ["SUBJ1", "SUBJ2"],
            "AGE": [25, 30],
        },
    )

    params = SqlOperationParams(
        domain="DM",
        target="AGE",
        standard="",
        standard_version="",
    )
    operation = SqlOperationsFactory.get_service("variable_count", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, 1)
