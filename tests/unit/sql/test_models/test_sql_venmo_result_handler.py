from types import SimpleNamespace

from cdisc_rules_engine.models.sql_venmo_result_handler import SqlVenmoResultHandler


class _Schema:
    @staticmethod
    def has_column(column):
        return column == "aeseq"

    @staticmethod
    def get_column_hash(column):
        return {"aeseq": "h_aeseq"}.get(column)


def test_build_clauses_resolves_domain_grouping_placeholder():
    handler = SqlVenmoResultHandler(
        output_container=[],
        dataset_metadata=SimpleNamespace(domain="AE", filename="ae.xpt", name="AE"),
        rule={"sensitivity": "group", "grouping_variables": ["--SEQ", "filter_by_dataset"]},
        dataset_id="ae",
        data_service=SimpleNamespace(),
    )

    distinct_clause, order_clause = handler._build_clauses(_Schema())

    assert distinct_clause == "DISTINCT ON (co.h_aeseq)"
    assert order_clause == "ORDER BY co.h_aeseq ASC, co.id ASC"
