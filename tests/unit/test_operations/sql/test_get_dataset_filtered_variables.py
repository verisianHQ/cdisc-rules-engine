from unittest.mock import patch
import pytest
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from .helpers import assert_operation_collection


@pytest.mark.parametrize(
    "domain, dataset_columns, mock_model_variables, key_name, key_value, expected",
    [
        (
            "AE",
            ["STUDYID", "DOMAIN", "USUBJID", "AETERM", "VISITNUM", "VISIT"],
            [
                {"name": "STUDYID", "role": "Identifier"},
                {"name": "DOMAIN", "role": "Identifier"},
                {"name": "USUBJID", "role": "Identifier"},
                {"name": "VISITNUM", "role": "Timing"},
                {"name": "VISIT", "role": "Timing"},
                {"name": "AETIMING", "role": "Timing"},
            ],
            "role",
            "Timing",
            ["VISITNUM", "VISIT"],
        ),
        (
            "AE",
            ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
            [
                {"name": "STUDYID", "role": "Identifier"},
                {"name": "DOMAIN", "role": "Identifier"},
                {"name": "USUBJID", "role": "Identifier"},
                {"name": "AETERM", "role": "Identifier"},
            ],
            "role",
            "Identifier",
            ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
        ),
        (
            "AE",
            ["STUDYID", "AETERM", "AESEQ"],
            [
                {"name": "--TERM", "role": "Topic"},
                {"name": "--SEQ", "role": "Identifier"},
                {"name": "STUDYID", "role": "Identifier"},
                {"name": "DOMAIN", "role": "Identifier"},
            ],
            "role",
            "Identifier",
            ["AESEQ", "STUDYID"],
        ),
        (
            "FA",
            ["STUDYID", "DOMAIN", "FAOBJ", "FASEQ", "USUBJID"],
            [
                {"name": "--OBJ", "role": "Topic"},
                {"name": "USUBJID", "role": "Identifier"},
                {"name": "TIMING_VAR1", "role": "Timing"},
                {"name": "DOMAIN", "role": "Identifier"},
                {"name": "STUDYID", "role": "Identifier"},
            ],
            "role",
            "Identifier",
            ["USUBJID", "DOMAIN", "STUDYID"],
        ),
        (
            "AE",
            ["STUDYID", "AETERM", "AESEQ"],
            [
                {"name": "STUDYID", "role": "Identifier"},
                {"name": "AETERM", "role": "Topic"},
            ],
            "role",
            "Timing",
            [],
        ),
    ],
)
def test_get_dataset_filtered_variables(domain, dataset_columns, mock_model_variables, key_name, key_value, expected):
    data_service = PostgresQLDataService.instance()

    column_data = {col: ["test"] for col in dataset_columns}
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=column_data,
        standards_context=DefaultStandardsContext(),
    )

    params = SqlOperationParams(
        domain=domain, target=None, standards_context=DefaultStandardsContext(), key_name=key_name, key_value=key_value
    )

    operation = SqlOperationsFactory.get_service("get_dataset_filtered_variables", params, data_service)

    with patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=mock_model_variables):
        result = operation.execute()

        assert_operation_collection(operation, result, expected, unsorted=True)
