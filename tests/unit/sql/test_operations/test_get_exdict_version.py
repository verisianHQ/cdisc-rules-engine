import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlOperationError
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory
from .helpers import assert_operation_constant


def _loinc_data_service(tmp_path, folder_name, extra_files=()):
    loinc_path = tmp_path / folder_name
    loinc_path.mkdir()
    (loinc_path / "Loinc.csv").write_text('"LOINC_NUM","COMPONENT","VersionLastChanged","STATUS"\n')
    for file_name in extra_files:
        (loinc_path / file_name).touch()
    return PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer({DictionaryTypes.LOINC.value: str(loinc_path)})
    )


def _get_version_operation(data_service, sdtm_standards_context):
    params = SqlOperationParams(
        domain="LB",
        target="FAKEVARIABLE",
        standards_context=sdtm_standards_context,
        external_dictionary_type=DictionaryTypes.LOINC.value,
    )
    return SqlOperationsFactory.get_service("get_external_dictionary_version", params, data_service)


@pytest.mark.parametrize(
    "folder_name",
    ["loinc_2.82", "Loinc_2.82", "my_loinc_2.82", "loinc_v2.82", "Loinc_2.82_Text", "loinc-2.82", "loinc 2.82"],
)
def test_get_exdict_version(sdtm_standards_context, tmp_path, folder_name):
    data_service = _loinc_data_service(tmp_path, folder_name)
    operation = _get_version_operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert_operation_constant(operation, result, expected="2.82")


@pytest.mark.parametrize("folder_name", ["loinc", "loinc_latest"])
def test_get_exdict_version_from_difference_report(sdtm_standards_context, tmp_path, folder_name):
    data_service = _loinc_data_service(tmp_path, folder_name, extra_files=["Loinc_2.80_DifferenceReport.pdf"])
    operation = _get_version_operation(data_service, sdtm_standards_context)
    result = operation.execute()

    assert_operation_constant(operation, result, expected="2.80")


@pytest.mark.parametrize(
    "folder_name, extra_files",
    [
        ("loinc", []),
        ("loinc_latest", []),
        ("loinc_v2", []),
        ("loinc", ["Loinc_latest_DifferenceReport.pdf"]),
    ],
)
def test_get_exdict_version_badly_named_folder_raises(sdtm_standards_context, tmp_path, folder_name, extra_files):
    data_service = _loinc_data_service(tmp_path, folder_name, extra_files=extra_files)
    operation = _get_version_operation(data_service, sdtm_standards_context)

    with pytest.raises(SqlOperationError, match="Version for external dictionary type loinc is not found"):
        operation.execute()


def test_get_exdict_version_dictionary_not_provided_raises(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    operation = _get_version_operation(data_service, sdtm_standards_context)

    with pytest.raises(SqlOperationError, match="Version for external dictionary type loinc is not found"):
        operation.execute()
