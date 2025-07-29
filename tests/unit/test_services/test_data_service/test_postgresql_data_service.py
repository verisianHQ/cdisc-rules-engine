from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_get_dataset_metadata_sql(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset, get_sample_supp_dataset])
    ds_metadata = sql_data_service.get_dataset_metadata(dataset_id="LB")
    assert 2 == len(ds_metadata.variables)
    ds_metadata = sql_data_service.get_dataset_metadata(dataset_id="SUPPDM")
    assert 3 == len(ds_metadata.variables)


def test_get_uploaded_dataset_ids(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset, get_sample_supp_dataset])
    assert 2 == len(sql_data_service.get_uploaded_dataset_ids())
