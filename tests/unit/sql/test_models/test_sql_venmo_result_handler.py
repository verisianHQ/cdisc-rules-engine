from types import SimpleNamespace

from cdisc_rules_engine.models.sql_venmo_result_handler import SqlVenmoResultHandler
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult


class _Schema:
    @staticmethod
    def has_column(column):
        return column.lower() in {"aeseq", "usubjid"}

    @staticmethod
    def get_column_hash(column):
        return {"aeseq": "h_aeseq", "usubjid": "h_usubjid"}.get(column.lower())


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


def test_build_select_cols_resolves_operation_parameters_against_outer_row():
    handler = SqlVenmoResultHandler(
        output_container=[],
        dataset_metadata=SimpleNamespace(domain="AE", filename="ae.xpt", name="AE"),
        rule={},
        dataset_id="ae",
        data_service=SimpleNamespace(),
        operation_variables={
            "$max_dsstdtc": SqlOperationResult(
                query="SELECT MAX(dsstdtc) FROM ds WHERE usubjid = $1",
                type="constant",
                subtype="Char",
                params={"$1": "USUBJID"},
            )
        },
    )

    select_cols = handler._build_select_cols({"$max_dsstdtc": True}, _Schema())

    assert select_cols == ['(SELECT MAX(dsstdtc) FROM ds WHERE usubjid = co.h_usubjid) AS "$max_dsstdtc"']


def test_build_validation_entities_matches_legacy_metadata_conversions():
    handler = SqlVenmoResultHandler(
        output_container=[],
        dataset_metadata=SimpleNamespace(domain="AE", filename="ae.xpt", name="AE"),
        rule={},
        dataset_id="ae",
        data_service=SimpleNamespace(),
    )

    entities = handler._build_validation_entities(
        [{"__source_row_number": 1.0, "__usubjid": None, "__aeseq": 2.0, "AETERM": "Headache"}],
        {"AETERM": True},
    )

    assert entities[0].to_representation() == {
        "dataset": "ae.xpt",
        "row": 1,
        "USUBJID": "None",
        "SEQ": 2,
        "value": {"AETERM": "Headache"},
    }


def test_build_validation_entities_omits_record_metadata_for_dataset_sensitivity():
    handler = SqlVenmoResultHandler(
        output_container=[],
        dataset_metadata=SimpleNamespace(domain="AE", filename="ae.xpt", name="AE"),
        rule={"sensitivity": "Dataset"},
        dataset_id="ae",
        data_service=SimpleNamespace(),
    )

    entities = handler._build_validation_entities(
        [{"__source_row_number": 1, "__usubjid": "CDISC001", "__aeseq": 2, "AETERM": "Headache"}],
        {"AETERM": True},
    )

    assert entities[0].to_representation() == {"dataset": "ae.xpt", "value": {"AETERM": "Headache"}}
