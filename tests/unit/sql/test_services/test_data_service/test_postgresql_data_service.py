import pickle

from cdisc_rules_engine.constants.data_structures import BDS
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.startup import populate_codelists
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.models.test_dataset import TestDataset as DatasetFixture
from cdisc_rules_engine.standards.adam_standards_context import AdamStandardsContext
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)


def write_ct_package(cache_dir, version_date: str, term_code: str):
    """Writes a minimal cached sdtmct package, in the cache pickle format."""
    with open(cache_dir / f"sdtmct-{version_date}.pkl", "wb") as f:
        pickle.dump(
            {
                "package": f"sdtmct-{version_date}",
                "submission_lookup": {},
                "CL9": {"name": "Codelist Nine", "submissionValue": "CL9", "terms": [{"conceptId": term_code}]},
            },
            f,
        )


def loaded_ct_versions(data_service: PostgresQLDataService) -> set:
    data_service.pgi.execute_sql(f"SELECT DISTINCT version_date FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}")
    return {row["version_date"] for row in data_service.pgi.fetch_all()}


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


def ts_dataset(records: list[tuple[str, str]]) -> DatasetFixture:
    return DatasetFixture.from_records(
        "TS",
        {
            "STUDYID": ["STUDY1"] * len(records),
            "TSPARMCD": ["PARM"] * len(records),
            "TSVCDREF": [reference for reference, _ in records],
            "TSVCDVER": [version for _, version in records],
        },
    )


def test_ct_packages_named_in_tsvcdver_are_loaded_with_the_datasets(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")
    write_ct_package(tmp_path, "1999-06-25", "C333333")
    dataset = ts_dataset(
        [
            ("CDISC", "1999-01-29"),
            ("CDISC CT", "1999-03-26"),
            ("SNOMED", "1999-06-25"),
            ("CDISC", "../not-a-date"),
            ("CDISC", ""),
        ]
    )

    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [dataset], sdtm_standards_context, cache_path=str(tmp_path)
    )

    assert loaded_ct_versions(data_service) == {"1999-01-29", "1999-03-26"}


def test_ct_version_columns_are_only_read_in_their_domain(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")
    not_ts_dataset = DatasetFixture.from_records("XX", {"TSVCDREF": ["CDISC"], "TSVCDVER": ["1999-01-29"]})

    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [not_ts_dataset], sdtm_standards_context, cache_path=str(tmp_path)
    )

    assert loaded_ct_versions(data_service) == {"1999-03-26"}


def test_numeric_tsvcdver_names_no_ct_package(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")
    dataset = DatasetFixture.from_records("TS", {"TSVCDREF": ["CDISC"], "TSVCDVER": [14273.0]})

    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [dataset], sdtm_standards_context, cache_path=str(tmp_path)
    )

    assert loaded_ct_versions(data_service) == {"1999-03-26"}


def test_warning_for_tsvcdver_ct_package_missing_from_cache(sdtm_standards_context, tmp_path, monkeypatch):
    warnings = []
    monkeypatch.setattr(populate_codelists.logger, "warning", lambda msg, *args, **kwargs: warnings.append(msg))
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    dataset = ts_dataset([("CDISC", "1999-01-29"), ("CDISC CT", "1999-06-25"), ("SNOMED", "1999-09-24")])

    PostgresQLDataService.from_list_of_testdatasets([dataset], sdtm_standards_context, cache_path=str(tmp_path))

    assert len(warnings) == 1
    assert "sdtmct-1999-06-25 referenced by TSVCDVER" in warnings[0]


def test_most_recent_cached_ct_package_is_loaded_when_none_is(sdtm_standards_context, tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")

    data_service = PostgresQLDataService.from_list_of_testdatasets(
        [ts_dataset([("SNOMED", "1999-01-29")])], sdtm_standards_context, cache_path=str(tmp_path)
    )

    assert loaded_ct_versions(data_service) == {"1999-03-26"}


def test_most_recent_cached_ct_package_is_not_added_to_declared_ones(tmp_path):
    write_ct_package(tmp_path, "1999-01-29", "C111111")
    write_ct_package(tmp_path, "1999-03-26", "C222222")
    data_service = PostgresQLDataService.instance(cache_path=str(tmp_path), codelists=["sdtmct-1999-01-29.pkl"])

    populate_codelists.populate_latest_codelist(data_service.pgi, data_service.cache_path, "sdtm")

    assert loaded_ct_versions(data_service) == {"1999-01-29"}
