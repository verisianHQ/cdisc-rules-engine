from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import assert_operation_parameterized_collection


def test_referenced_domain_variable_names(sdtm_standards_context):
    """
    A schema-only lookup: for each RDOMAIN value on a CO-like row, returns the column
    names of whichever dataset in the study has that domain - independent of whether
    IDVAR (the thing this is used to validate) names a real column or not.
    """
    data_service = PostgresQLDataService.instance()

    PostgresQLDataService.add_test_dataset(
        data_service,
        "ae",
        {
            "STUDYID": ["S1", "S1"],
            "USUBJID": ["U1", "U2"],
            "AESEQ": ["1", "2"],
            "AETERM": ["Headache", "Nausea"],
        },
        sdtm_standards_context,
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        "lb",
        {
            "STUDYID": ["S1", "S1"],
            "USUBJID": ["U1", "U2"],
            "LBSEQ": ["1", "2"],
            "LBTEST": ["Glucose", "Sodium"],
        },
        sdtm_standards_context,
    )
    co_table = PostgresQLDataService.add_test_dataset(
        data_service,
        "co",
        {
            "STUDYID": ["S1", "S1", "S1"],
            "RDOMAIN": ["AE", "LB", "ZZ"],
            "USUBJID": ["U1", "U2", "U3"],
            "IDVAR": ["AESEQ", "BOGUSVAR", "AESEQ"],
        },
        sdtm_standards_context,
    )

    params = SqlOperationParams(
        domain="co",
        target="RDOMAIN",
        table=co_table.hash,
        standards_context=sdtm_standards_context,
    )
    operation = SqlOperationsFactory.get_service("referenced_domain_variable_names", params, data_service)
    result = operation.execute()

    assert result.type == "collection"
    assert result.params == {"$1": "RDOMAIN"}

    assert_operation_parameterized_collection(
        operation,
        result,
        [
            {"params": {"$1": "AE"}, "value": ["STUDYID", "USUBJID", "AESEQ", "AETERM"]},
            {"params": {"$1": "LB"}, "value": ["STUDYID", "USUBJID", "LBSEQ", "LBTEST"]},
            {"params": {"$1": "ZZ"}, "value": []},
        ],
        unsorted=True,
    )
