import pickle

import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.get_codelist_attributes import (
    SqlGetCodelistAttributesOperation,
)
from .helpers import assert_operation_collection, assert_operation_parameterized_collection


def setup_codelist_table(data_service: PostgresQLDataService):
    table_name = StaticTables.IG_CODELIST_TABLE_NAME.value
    schema = SqlTableSchema.static(table_name)
    schema.add_column(SqlColumnSchema("standard_type", "standard_type", "Char"))
    schema.add_column(SqlColumnSchema("version_date", "version_date", "Char"))
    schema.add_column(SqlColumnSchema("item_code", "item_code", "Char"))
    schema.add_column(SqlColumnSchema("value", "value", "Char"))
    schema.add_column(SqlColumnSchema("codelist_code", "codelist_code", "Char"))
    schema.add_column(SqlColumnSchema("name", "name", "Char"))
    schema.add_column(SqlColumnSchema("term", "term", "Char"))

    data_service.pgi.create_table(schema)

    data = [
        {
            "standard_type": "sdtm",
            "version_date": "2020-03-27",
            "item_code": "C1234",
            "value": "Signification A",
            "codelist_code": "CL1",
            "name": "Codelist One",
            "term": "Term A",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2020-03-27",
            "item_code": "C5678",
            "value": "Signification B",
            "codelist_code": "CL1",
            "name": "Codelist One",
            "term": "Term B",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2021-12-17",
            "item_code": "C999",
            "value": "Signification C",
            "codelist_code": "CL2",
            "name": "Codelist Two",
            "term": "Term C",
        },
        {
            "standard_type": "sdtm",
            "version_date": "2021-12-17",
            "item_code": "C999",
            "value": "Signification D",
            "codelist_code": "CL2",
            "name": "Codelist Two",
            "term": "Term D",
        },
    ]
    data_service.pgi.insert_data(table_name, data)


def test_get_codelist_attributes_term_ccode(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2020-03-27")
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["C1234", "C5678"], unsorted=True)


def test_get_codelist_attributes_term_signification(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2021-12-17")
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term Signification",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["Signification C", "Signification D"], unsorted=True)


def setup_data_table_with_version_column(data_service: PostgresQLDataService, table_name: str):
    schema = SqlTableSchema.static(table_name)
    schema.add_column(SqlColumnSchema("studydate", "studydate", "Char"))
    data_service.pgi.create_table(schema)
    data_service.pgi.insert_data(table_name, [{"studydate": "2021-12-17"}])


def test_get_codelist_attributes_column_referenced_version_uses_table_not_domain(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert result.params == {"$ct_version": "studydate"}
    assert_operation_parameterized_collection(
        operation,
        result,
        [{"params": {"$ct_version": "2021-12-17"}, "value": ["C999"]}],
        unsorted=True,
    )


def test_get_codelist_attributes_column_reference_takes_precedence_over_provided_codelists(
    sdtm_standards_context,
):
    data_service = PostgresQLDataService.instance(provided_codelists="sdtmct-2020-03-27")
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert result.params == {"$ct_version": "studydate"}
    assert_operation_parameterized_collection(
        operation,
        result,
        [
            {"params": {"$ct_version": "2021-12-17"}, "value": ["C999"]},
            {"params": {"$ct_version": None}, "value": ["C1234", "C5678"]},
            {"params": {"$ct_version": ""}, "value": ["C1234", "C5678"]},
        ],
        unsorted=True,
    )


def test_loading_ct_packages_referenced_by_column(sdtm_standards_context, tmp_path):
    version_date = "1999-01-29"
    write_ct_package(tmp_path, version_date, "C424242")
    data_service = PostgresQLDataService.instance(cache_path=str(tmp_path))
    schema = SqlTableSchema.static("ts1")
    schema.add_column(SqlColumnSchema("tsvcdver", "tsvcdver", "Char"))
    data_service.pgi.create_table(schema)
    data_service.pgi.insert_data("ts1", [{"tsvcdver": version_date}, {"tsvcdver": "../not-a-date"}, {"tsvcdver": ""}])

    params = SqlOperationParams(
        domain="ts",
        table="ts1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="tsvcdver",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_parameterized_collection(
        operation,
        result,
        [{"params": {"$ct_version": version_date}, "value": ["CL9", "C424242"]}],
        unsorted=True,
    )


def test_get_codelist_attributes_without_versions_uses_all_loaded_ct_packages(sdtm_standards_context):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["C1234", "C5678", "C999"], unsorted=True)


def test_get_codelist_attributes_empty_column_version(
    sdtm_standards_context,
):
    data_service = PostgresQLDataService.instance()
    setup_codelist_table(data_service)
    setup_data_table_with_version_column(data_service, "ae1")

    params = SqlOperationParams(
        domain="ae",
        table="ae1",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="studydate",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_parameterized_collection(
        operation,
        result,
        [
            {"params": {"$ct_version": "2020-03-27"}, "value": ["C1234", "C5678"]},
            {"params": {"$ct_version": None}, "value": []},
            {"params": {"$ct_version": ""}, "value": []},
        ],
        unsorted=True,
    )


def write_ct_package(cache_dir, version_date: str, term_code: str):
    with open(cache_dir / f"sdtmct-{version_date}.pkl", "wb") as f:
        pickle.dump(
            {
                "package": f"sdtmct-{version_date}",
                "submission_lookup": {},
                "CL9": {"name": "Codelist Nine", "submissionValue": "CL9", "terms": [{"conceptId": term_code}]},
            },
            f,
        )


def test_loading_most_recent_cached_ct_package_by_default(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")
    data_service = PostgresQLDataService.instance(cache_path=str(tmp_path))

    params = SqlOperationParams(
        domain="dataset",
        target="column",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
    )

    operation = SqlGetCodelistAttributesOperation(params, data_service)
    result = operation.execute()

    assert_operation_collection(operation, result, ["CL9", "C222222"], unsorted=True)


def test_warning_for_column_referenced_ct_package_missing_from_cache(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    data_service = PostgresQLDataService.instance(cache_path=str(tmp_path))
    schema = SqlTableSchema.static("ts1")
    schema.add_column(SqlColumnSchema("tsvcdref", "tsvcdref", "Char"))
    schema.add_column(SqlColumnSchema("tsvcdver", "tsvcdver", "Char"))
    data_service.pgi.create_table(schema)
    data_service.pgi.insert_data(
        "ts1",
        [
            {"tsvcdref": "CDISC", "tsvcdver": "1999-01-29"},
            {"tsvcdref": "CDISC CT", "tsvcdver": "1999-06-25"},
            {"tsvcdref": "SNOMED", "tsvcdver": "1999-09-24"},
        ],
    )

    params = SqlOperationParams(
        domain="ts",
        table="ts1",
        target="tsvcdref",
        standards_context=sdtm_standards_context,
        ct_attribute="Term CCODE",
        ct_version="tsvcdver",
    )

    with pytest.warns(UserWarning, match="sdtmct-1999-06-25 referenced by column tsvcdver") as record:
        SqlGetCodelistAttributesOperation(params, data_service).execute()

    assert len(record) == 1
