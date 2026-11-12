from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.referenced_domain_variable_names import (
    SqlReferencedDomainVariableNamesOperation,
)
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


def _operation(data_service, sdtm_standards_context):
    params = SqlOperationParams(domain="co", target="RDOMAIN", standards_context=sdtm_standards_context)
    return SqlOperationsFactory.get_service("referenced_domain_variable_names", params, data_service)


def _lookup(operation, result, rdomain):
    """Run the lookup for one record's RDOMAIN, the way the compiler binds $1 (a column, or NULL)."""
    literal = "NULL" if rdomain is None else "'" + rdomain.replace("'", "''") + "'"
    operation.data_service.pgi.execute_sql(result.query.replace("$1", literal))
    return [row["value"] for row in operation.data_service.pgi.fetch_all()]


def test_referenced_domain_variable_names_is_a_jsonb_lookup(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, "ae", {"STUDYID": ["S1"], "AESEQ": ["1"]}, sdtm_standards_context
    )
    operation = _operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert (result.type, result.subtype, result.params) == ("collection", "Char", {"$1": "RDOMAIN"})
    assert "jsonb" in result.query
    assert "VALUES" not in result.query
    assert result.query.count("$1") == 1


def test_referenced_domain_variable_names_unknown_null_or_differently_cased_domain(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, "ae", {"STUDYID": ["S1"], "AESEQ": ["1"]}, sdtm_standards_context
    )
    operation = _operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert _lookup(operation, result, "AE") == ["STUDYID", "AESEQ"]
    assert _lookup(operation, result, "ZZ") == []
    assert _lookup(operation, result, None) == []
    assert _lookup(operation, result, "") == []
    assert _lookup(operation, result, "ae") == []


def test_referenced_domain_variable_names_first_dataset_of_a_domain_wins(sdtm_standards_context):
    """lb and lbch both have domain LB: only the first one's variables are listed, once each."""
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, "lb", {"STUDYID": ["S1"], "LBSEQ": ["1"]}, sdtm_standards_context
    )
    PostgresQLDataService.add_test_dataset(
        data_service, "lbch", {"STUDYID": ["S1"], "LBCHVAR": ["1"]}, sdtm_standards_context
    )
    operation = _operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert _lookup(operation, result, "LB") == ["STUDYID", "LBSEQ"]


def test_referenced_domain_variable_names_empty_study(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    operation = _operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert (result.type, result.params) == ("collection", {"$1": "RDOMAIN"})
    assert _lookup(operation, result, "AE") == []
    assert _lookup(operation, result, None) == []


def test_referenced_domain_variable_names_escapes_the_jsonb_literal(sdtm_standards_context, monkeypatch):
    """Quotes and backslashes survive both the JSON encoding and the SQL string literal."""
    names = ["STUDYID", "AE'VAR", 'AE"VAR', "AE\\VAR"]
    monkeypatch.setattr(
        SqlReferencedDomainVariableNamesOperation, "_domain_variables", lambda self: {"AE": names, "O'X": ["X"]}
    )
    operation = _operation(PostgresQLDataService.instance(), sdtm_standards_context)
    result = operation.execute()

    assert _lookup(operation, result, "AE") == names
    assert _lookup(operation, result, "O'X") == ["X"]
