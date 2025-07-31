from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_db_cache_initialization(get_sample_supp_dataset, get_sample_lb_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset, get_sample_lb_dataset], None)
    assert "SUPPDM" == ds.cache.get_tables().get("SUPPDM")
    assert "LB" == ds.cache.get_tables().get("LB")

    assert "SUPPDM" == ds.cache.get_db_table_cache("SUPPDM").get("db_table")
    assert 3 == len(ds.cache.get_db_table_cache("SUPPDM").get("columns"))
    assert "LB" == ds.cache.get_db_table_cache("LB").get("db_table")
    assert 2 == len(ds.cache.get_db_table_cache("LB").get("columns"))

    assert "SUPPDM" == ds.cache.get_db_table("SUPPDM")
    assert "LB" == ds.cache.get_db_table("LB")

    assert 3 == len(ds.cache.get_columns("SUPPDM"))
    assert 2 == len(ds.cache.get_columns("LB"))

    assert "DOMAIN" == ds.cache.get_db_column("SUPPDM", "DOMAIN")
    assert "RDOMAIN" == ds.cache.get_db_column("SUPPDM", "RDOMAIN")
    assert "LBSEQ" == ds.cache.get_db_column("SUPPDM", "LBSEQ")

    assert "DOMAIN" == ds.cache.get_db_column("LB", "DOMAIN")
    assert "LBSEQ" == ds.cache.get_db_column("LB", "LBSEQ")
