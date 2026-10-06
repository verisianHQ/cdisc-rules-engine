from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.data_service.startup.populate_codelists import add_extensible_terms
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.define_extended_terms_in_ct import (
    SqlDefineExtendedTermsInCtOperation,
)
from .helpers import assert_operation_collection


def _library_term(version_date: str, codelist_code: str, name: str, value: str, synonym: str = None) -> dict:
    return {
        "standard_type": "sdtm",
        "version_date": version_date,
        "codelist_code": codelist_code,
        "extensible": "Yes",
        "name": name,
        "value": value,
        "synonym": synonym,
    }


def setup_codelist_table(data_service: PostgresQLDataService, extensible_terms: dict):
    table_name = StaticTables.IG_CODELIST_TABLE_NAME.value
    schema = SqlTableSchema.static(table_name)
    for column in ["standard_type", "version_date", "codelist_code", "extensible", "name", "value", "synonym"]:
        schema.add_column(SqlColumnSchema(column, column, "Char"))
    data_service.pgi.create_table(schema)

    data = [
        _library_term("2025-03-28", "C116104", "Nervous System Findings Test Code", "ABRIAL"),
        _library_term("2025-03-28", "C117743", "Ophthalmic Exam Test Code", "INTP", "Interpretation"),
        _library_term("2025-03-28", "C117742", "Ophthalmic Exam Test Name", "Interpretation", "Interpretation"),
        _library_term(
            "2025-03-28", "C116103", "Nervous System Findings Test Name", "ABR Wave I, Absolute Latency", "ABR1; ABRI"
        ),
        _library_term("2024-12-20", "C116104", "Nervous System Findings Test Code", "OLDTERM"),
    ]
    data_service.pgi.insert_data(table_name, data)
    add_extensible_terms(data_service.pgi, extensible_terms)


def _execute(data_service: PostgresQLDataService, standards_context, **kwargs):
    params = SqlOperationParams(domain="dataset", target=None, standards_context=standards_context, **kwargs)
    operation = SqlDefineExtendedTermsInCtOperation(params, data_service)
    return operation, operation.execute()


def test_define_extended_terms_in_ct_only_matches_within_same_codelist(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2025-03-28")
    setup_codelist_table(
        data_service,
        {
            "Nervous System Test Code": {"codelist": "C116104", "extended_values": ["INTP"]},
            "Ophthalmic Exam Test Code": {"codelist": "C117743", "extended_values": ["INTP", "NEWTERM"]},
        },
    )

    operation, result = _execute(data_service, sdtm_standards_context)

    assert_operation_collection(operation, result, ["Ophthalmic Exam Test Code (C117743): INTP"])


def test_define_extended_terms_in_ct_matches_synonyms(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2025-03-28")
    setup_codelist_table(
        data_service,
        {
            "Nervous System Test": {"codelist": "C116103", "extended_values": ["ABRI", "ABR"]},
            "Ophthalmic Exam Test Code": {"codelist": "C117743", "extended_values": ["Interpretation"]},
        },
    )

    operation, result = _execute(data_service, sdtm_standards_context)

    assert_operation_collection(
        operation,
        result,
        ["Nervous System Test (C116103): ABRI", "Ophthalmic Exam Test Code (C117743): Interpretation"],
    )


def test_define_extended_terms_in_ct_uses_provided_ct_version(sdtm_standards_context):
    extensible_terms = {"Nervous System Test Code": {"codelist": "C116104", "extended_values": ["OLDTERM"]}}

    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2025-03-28")
    setup_codelist_table(data_service, extensible_terms)
    operation, result = _execute(data_service, sdtm_standards_context)
    assert_operation_collection(operation, result, [])

    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2024-12-20")
    setup_codelist_table(data_service, extensible_terms)
    operation, result = _execute(data_service, sdtm_standards_context)
    assert_operation_collection(operation, result, ["Nervous System Test Code (C116104): OLDTERM"])


def test_define_extended_terms_in_ct_case_sensitivity(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2025-03-28")
    setup_codelist_table(
        data_service, {"Ophthalmic Exam Test Code": {"codelist": "C117743", "extended_values": ["intp"]}}
    )

    operation, result = _execute(data_service, sdtm_standards_context)
    assert_operation_collection(operation, result, [])

    operation, result = _execute(data_service, sdtm_standards_context, case_sensitive=False)
    assert_operation_collection(operation, result, ["Ophthalmic Exam Test Code (C117743): intp"])
