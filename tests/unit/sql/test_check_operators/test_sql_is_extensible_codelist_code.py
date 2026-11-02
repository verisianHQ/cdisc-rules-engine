from cdisc_rules_engine.data_service.startup.populate_codelists import add_extensible_terms, codelist_schema
from cdisc_rules_engine.enums.static_tables import StaticTables

from .helpers import assert_series_equals, create_sql_operators


def test_a_defines_extended_values_do_not_make_a_codelist_extensible():
    sql_ops = create_sql_operators({"CODE": ["C66742", "C66741", "C99999"]})
    pgi = sql_ops.data["data_service"].pgi
    pgi.create_table(codelist_schema())
    pgi.insert_data(
        StaticTables.IG_CODELIST_TABLE_NAME.value,
        [
            {"standard_type": "sdtm", "version_date": "2024-03-29", "codelist_code": "C66742", "extensible": "No"},
            {"standard_type": "sdtm", "version_date": "2024-03-29", "codelist_code": "C66741", "extensible": "Yes"},
        ],
    )
    add_extensible_terms(pgi, {"No Yes Response": {"codelist": "C66742", "extended_values": ["ZZ"]}})
    assert_series_equals(sql_ops.is_extensible_codelist_code({"target": "CODE"}), [False, True, False])
    assert_series_equals(sql_ops.is_not_extensible_codelist_code({"target": "CODE"}), [True, False, True])
