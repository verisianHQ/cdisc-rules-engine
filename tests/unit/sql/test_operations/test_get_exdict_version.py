from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory
from .helpers import assert_operation_constant


def test_get_exdict_version(sdtm_standards_context, tmp_path):
    loinc_path = tmp_path / "loinc_2.82"
    loinc_path.mkdir()
    (loinc_path / "Loinc.csv").write_text('"LOINC_NUM","COMPONENT","VersionLastChanged","STATUS"\n')
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer({DictionaryTypes.LOINC.value: str(loinc_path)})
    )
    params = SqlOperationParams(
        domain="LB",
        target="FAKEVARIABLE",
        standards_context=sdtm_standards_context,
        external_dictionary_type=DictionaryTypes.LOINC.value,
    )
    operation = SqlOperationsFactory.get_service("get_external_dictionary_version", params, data_service)
    result = operation.execute()

    assert_operation_constant(operation, result, expected="2.82")
