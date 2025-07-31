from cdisc_rules_engine.data_service.db_cache import DBCache
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_db_cache_initialization(get_sample_supp_dataset, get_sample_lb_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset, get_sample_lb_dataset], None)
    assert "suppdm" == ds.cache.get_tables().get("suppdm")
    assert "lb" == ds.cache.get_tables().get("lb")

    assert "suppdm" == ds.cache.get_db_table_cache("suppdm").get("db_table")
    assert 3 == len(ds.cache.get_db_table_cache("suppdm").get("columns"))
    assert "lb" == ds.cache.get_db_table_cache("lb").get("db_table")
    assert 2 == len(ds.cache.get_db_table_cache("lb").get("columns"))

    assert "suppdm" == ds.cache.get_db_table_hash("suppdm")
    assert "lb" == ds.cache.get_db_table_hash("lb")

    assert 3 == len(ds.cache.get_columns("suppdm"))
    assert 2 == len(ds.cache.get_columns("lb"))

    assert "DOMAIN" == ds.cache.get_db_column_hash("suppdm", "DOMAIN")
    assert "RDOMAIN" == ds.cache.get_db_column_hash("suppdm", "RDOMAIN")
    assert "LBSEQ" == ds.cache.get_db_column_hash("suppdm", "LBSEQ")

    assert "DOMAIN" == ds.cache.get_db_column_hash("lb", "DOMAIN")
    assert "LBSEQ" == ds.cache.get_db_column_hash("lb", "LBSEQ")


def test_empty_cache():
    cache = DBCache.empty_cache()
    assert {} == cache.get_tables()
    assert cache.get_tables().get("suppdm") is None
    assert cache.get_db_table_cache("suppdm") is None
    assert cache.get_db_table_hash("suppdm") is None
    assert {} == cache.get_columns("suppdm")
    assert cache.get_db_column_hash("suppdm", "domain") is None


def test_add_db_column_if_missing(get_sample_supp_dataset, get_sample_lb_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset, get_sample_lb_dataset], None)
    assert "lbseq" == ds.cache.add_db_column_if_missing(table_key="lb", column_key="LBSEQ")
