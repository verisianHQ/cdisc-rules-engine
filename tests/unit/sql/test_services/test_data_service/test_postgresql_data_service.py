from cdisc_rules_engine.constants.data_structures import BDS
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.models.test_dataset import TestDataset as DatasetFixture
from cdisc_rules_engine.standards.adam_standards_context import AdamStandardsContext
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)


def test_get_dataset_metadata_sql(get_sample_lb_dataset, get_sample_supp_dataset, sdtm_standards_context):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_lb_dataset, get_sample_supp_dataset], sdtm_standards_context
    )
    ds_metadata = sql_data_service.get_dataset_metadata("lb")
    assert 2 == len(ds_metadata.variables)
    assert ds_metadata.name == "lb"
    assert ds_metadata.domain == "LB"
    assert ds_metadata.rdomain == ""
    assert not ds_metadata.is_supp

    ds_metadata = sql_data_service.get_dataset_metadata("suppdm")
    assert 9 == len(ds_metadata.variables)
    assert ds_metadata.name == "suppdm"
    assert ds_metadata.domain == "SUPPDM"
    assert "DM" == ds_metadata.rdomain
    assert ds_metadata.is_supp


def test_get_uploaded_dataset_ids(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_lb_dataset, get_sample_supp_dataset], DefaultStandardsContext()
    )
    assert 2 == len(sql_data_service.get_uploaded_dataset_ids())


def test_adam_dataset_class_assigned_during_sql_load():
    dataset = DatasetFixture.from_records(
        "ADVS",
        {
            "STUDYID": ["STUDY1"],
            "USUBJID": ["STUDY1-001"],
            "PARAMCD": ["SYSBP"],
            "AVAL": [120],
        },
    )
    context = AdamStandardsContext(LibraryMetadataContainer())

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([dataset], context)

    assert sql_data_service.get_dataset_metadata("ADVS").dataset_class == BDS


def test_insert_empty_data_creates_no_rows_and_does_not_raise():
    data_service = PostgresQLDataService.instance()
    table_name = "empty_table"
    schema = SqlTableSchema.from_data(table_name, {"col1": "sample"}, data_service.pgi)
    data_service.pgi.create_table(schema)

    inserted_rows = data_service.pgi.insert_data(table_name, [])

    assert inserted_rows == 0

    table_hash = data_service.pgi.schema.get_table_hash(table_name)
    data_service.pgi.execute_sql(f"SELECT COUNT(*) AS cnt FROM {table_hash}")
    result = data_service.pgi.fetch_one()
    assert result["cnt"] == 0
