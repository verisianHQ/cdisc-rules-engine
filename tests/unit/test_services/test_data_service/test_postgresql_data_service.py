from cdisc_rules_engine.interfaces.PostgresQLDataService import PostgresQLDataService


def test_is_suppplemental_dataset(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], library_metadata=None)
    assert not sql_data_service.is_supplemental_dataset("lb.xpt")

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset], library_metadata=None)
    assert sql_data_service.is_supplemental_dataset("suppdm.xpt")


def test_get_domain(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], library_metadata=None)
    assert "LB" == sql_data_service.get_domain("lb.xpt")

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset], library_metadata=None)
    assert "SUPPDM" == sql_data_service.get_domain("suppdm.xpt")


def test_get_rdomain(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], library_metadata=None)
    assert sql_data_service.get_rdomain("lb.xpt") is None

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset], library_metadata=None)
    assert "DM" == sql_data_service.get_rdomain("suppdm.xpt")
