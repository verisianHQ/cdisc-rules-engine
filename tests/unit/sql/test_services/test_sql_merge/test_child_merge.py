"""
Tests for SQL child merge implementation.
"""

import pandas as pd
import pytest

from cdisc_rules_engine.data_service.merges.child import SqlChildMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.util import numeric_aware_equals_sql, numeric_aware_key_sql
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError, MatchDatasetNotFoundError
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

SIMPLE_RDOMAIN_DATA = {
    "child": {
        "STUDYID": ["S1", "S1", "S1"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["U1", "U2", "U3"],
        "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
        "IDVARVAL": ["1", "1", "1"],
        "QNAM": ["SEV", "SEV", "SEV"],
        "QVAL": ["MILD", "MODERATE", "SEVERE"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "AESEQ": ["1", "1"],
        "AETERM": ["Headache", "Nausea"],
        "AGE": ["45", "52"],
    },
}

PATTERN_REPLACEMENT_DATA = {
    "child": {
        "STUDYID": ["S1", "S1"],
        "RDOMAIN": ["LB", "LB"],
        "USUBJID": ["U1", "U2"],
        "IDVAR": ["LBSEQ", "LBSEQ"],
        "IDVARVAL": ["1", "2"],
        "QNAM": ["REPEAT", "REPEAT"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "LBSEQ": ["1", "2"],
        "LBTEST": ["Glucose", "Sodium"],
    },
}

MATCH_KEY_FALLBACK_DATA = {
    "child": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "SEQ": ["1", "2"],
        "CHILDCOL": ["A", "B"],
    },
    "parent": {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "SEQ": ["1", "2"],
        "PARENTCOL": ["X", "Y"],
    },
}


@pytest.mark.parametrize(
    "data, expected, child_domain",
    [
        (
            SIMPLE_RDOMAIN_DATA,
            [
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U1",
                    "qnam": "SEV",
                    "qval": "MILD",
                    "aeterm": "Headache",
                    "age": "45",
                },
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U2",
                    "qnam": "SEV",
                    "qval": "MODERATE",
                    "aeterm": "Nausea",
                    "age": "52",
                },
                {
                    "studyid": "S1",
                    "rdomain": "AE",
                    "usubjid": "U3",
                    "qnam": "SEV",
                    "qval": "SEVERE",
                    "aeterm": None,
                    "age": None,
                },
            ],
            "SUPPAE",
        ),
    ],
)
def test_child_merge_with_rdomain(sdtm_standards_context, data, expected, child_domain):
    """Test basic child merge using RDOMAIN column to find parent."""
    ds = PostgresQLDataService.instance()

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", data["child"], sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", data["parent"], sdtm_standards_context)

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain=child_domain,
        datasets=ds.datasets,
        merge_spec=merge_spec,
    )

    assert result_schema.has_column("studyid")
    assert result_schema.has_column("rdomain")
    assert result_schema.has_column("usubjid")
    assert result_schema.has_column("qnam")
    assert result_schema.has_column("qval")
    assert result_schema.has_column("ae.aeterm") or result_schema.has_column("aeterm")
    assert result_schema.has_column("ae.age") or result_schema.has_column("age")

    aeterm_col = result_schema.get_column_hash("aeterm")
    age_col = result_schema.get_column_hash("age")

    ds.pgi.execute_sql(
        f"""SELECT
                studyid,
                rdomain,
                usubjid,
                qnam,
                qval,
                {aeterm_col} as aeterm,
                {age_col} as age
            FROM {result_schema.hash}
            ORDER BY usubjid"""
    )
    actual = pd.DataFrame(ds.pgi.fetch_all())
    expected_df = pd.DataFrame(expected)

    assert expected_df.equals(actual), f"Expected:\n{expected_df}\n\nActual:\n{actual}"


def test_child_merge_with_pattern_replacement(sdtm_standards_context):
    """Test child merge with -- pattern replacement in match keys."""
    ds = PostgresQLDataService.instance()
    data = PATTERN_REPLACEMENT_DATA

    child_schema = PostgresQLDataService.add_test_dataset(ds, "supplb", data["child"], sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "lb", data["parent"], sdtm_standards_context)

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "--SEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPLB",
        datasets=ds.datasets,
        merge_spec=merge_spec,
    )

    assert result_schema is not None
    ds.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = ds.pgi.fetch_all()
    assert result[0]["count"] == 2

    lbtest_col = result_schema.get_column_hash("lbtest")
    ds.pgi.execute_sql(
        f"""SELECT
                usubjid,
                {lbtest_col} as lbtest
            FROM {result_schema.hash}
            ORDER BY usubjid"""
    )
    results = ds.pgi.fetch_all()
    assert len(results) == 2
    assert results[0]["lbtest"] == "Glucose"
    assert results[1]["lbtest"] == "Sodium"


def test_child_merge_run_twice(sdtm_standards_context):
    """Test that running same child merge twice returns cached result."""
    ds = PostgresQLDataService.instance()
    data = SIMPLE_RDOMAIN_DATA

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", data["child"], sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", data["parent"], sdtm_standards_context)

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result1 = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=ds.datasets,
        merge_spec=merge_spec,
    )

    result2 = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=ds.datasets,
        merge_spec=merge_spec,
    )

    assert result1 == result2
    assert result1.name == result2.name


def test_child_merge_no_parent_found(sdtm_standards_context):
    """A child whose RDOMAIN names only missing datasets skips, like any missing match dataset."""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1"],
        "RDOMAIN": ["NONEXISTENT"],
        "USUBJID": ["U1"],
    }
    child_schema = PostgresQLDataService.add_test_dataset(ds, "child", child_data, sdtm_standards_context)

    datasets = []

    merge_spec = {"match_key": ["STUDYID", "USUBJID"]}

    with pytest.raises(MatchDatasetNotFoundError, match="Match dataset NONEXISTENT is not present"):
        SqlChildMerge.perform_merge(
            pgi=ds.pgi,
            child=child_schema,
            child_domain="CHILD",
            datasets=datasets,
            merge_spec=merge_spec,
        )


@pytest.mark.parametrize(
    "child_data",
    [
        {"STUDYID": [], "DOMAIN": [], "USUBJID": [], "COSEQ": [], "COVAL": []},
        {"STUDYID": [], "DOMAIN": [], "RDOMAIN": [], "USUBJID": [], "COVAL": []},
        {"STUDYID": ["S1"], "DOMAIN": ["CO"], "USUBJID": ["U1"], "COSEQ": [1], "COVAL": ["General comment"]},
    ],
)
def test_child_merge_child_naming_no_parent_is_unchanged(sdtm_standards_context, child_data):
    """A child with no RDOMAIN values links to nothing, so it is returned as is rather than raising."""
    ds = PostgresQLDataService.instance()
    child_schema = PostgresQLDataService.add_test_dataset(ds, "co", child_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(
        ds, "ae", {"STUDYID": ["S1"], "USUBJID": ["U1"], "AESEQ": ["1"]}, sdtm_standards_context
    )

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="CO",
        datasets=ds.datasets,
        merge_spec={"match_key": ["USUBJID"]},
    )

    assert result_schema is child_schema


def test_child_merge_missing_match_key(sdtm_standards_context):
    """A non-relational child without one of the match keys can't be merged, so the rule is skipped."""
    ds = PostgresQLDataService.instance()

    child_data = {"STUDYID": ["S1"], "RDOMAIN": ["AE"], "USUBJID": ["U1"]}
    parent_data = {"STUDYID": ["S1"], "USUBJID": ["U1"], "AESEQ": ["1"]}
    child_schema = PostgresQLDataService.add_test_dataset(ds, "child", child_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", parent_data, sdtm_standards_context)

    with pytest.raises(ColumnNotFoundError, match="Column 'AESEQ' not found in table 'child'"):
        SqlChildMerge.perform_merge(
            pgi=ds.pgi,
            child=child_schema,
            child_domain="CHILD",
            datasets=ds.datasets,
            merge_spec={"match_key": ["USUBJID", "AESEQ"]},
        )


def test_child_merge_unmatched_child_rows(sdtm_standards_context):
    """Test that unmatched child rows are preserved with NULL parent values (LEFT JOIN)."""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["U1", "U2", "U3"],
        "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
        "IDVARVAL": ["1", "1", "1"],
    }
    parent_data = {
        "STUDYID": ["S1"],
        "USUBJID": ["U1"],
        "AESEQ": ["1"],
        "AETERM": ["Headache"],
    }

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", child_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", parent_data, sdtm_standards_context)

    merge_spec = {"match_key": ["STUDYID", "USUBJID", {"left": "IDVARVAL", "right": "AESEQ"}]}

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=ds.datasets,
        merge_spec=merge_spec,
    )

    ds.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = ds.pgi.fetch_all()
    assert result[0]["count"] == 3

    aeterm_col = result_schema.get_column_hash("aeterm")
    ds.pgi.execute_sql(
        f"""SELECT
                usubjid,
                {aeterm_col} as aeterm
            FROM {result_schema.hash}
            WHERE usubjid IN ('U2', 'U3')
            ORDER BY usubjid"""
    )
    unmatched = ds.pgi.fetch_all()
    assert len(unmatched) == 2
    assert unmatched[0]["aeterm"] is None
    assert unmatched[1]["aeterm"] is None


def test_child_merge_idvar_numeric_parent_with_scientific_notation(sdtm_standards_context):
    """Test that child merge correctly handles scientific notation"""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["U1", "U2", "U3"],
        "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
        "IDVARVAL": ["1000000000000000", "1E5", "999"],
        "QNAM": ["SEV", "SEV", "SEV"],
        "QVAL": ["MILD", "MODERATE", "SEVERE"],
    }
    parent_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "USUBJID": ["U1", "U2", "U3"],
        "AESEQ": [1000000000000000, 100000, 1],
        "AETERM": ["Big", "Sci", "NoMatch"],
    }

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", child_data, sdtm_standards_context)
    parent_schema = PostgresQLDataService.add_test_dataset(ds, "ae", parent_data, sdtm_standards_context)
    assert parent_schema.get_column("aeseq").type == "Num"

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=ds.datasets,
        merge_spec={},
    )

    aeterm_col = result_schema.get_column_hash("aeterm")
    ds.pgi.execute_sql(f"SELECT usubjid, {aeterm_col} as aeterm FROM {result_schema.hash} ORDER BY usubjid")
    results = {r["usubjid"]: r["aeterm"] for r in ds.pgi.fetch_all()}

    assert results["U1"] == "Big"
    assert results["U2"] == "Sci"
    assert results["U3"] is None


def test_child_merge_idvarval_matched_per_idvar_numerically_or_as_text(sdtm_standards_context):
    """Each row matches only the column its own IDVAR names, numerically when both sides are numbers."""
    ds = PostgresQLDataService.instance()

    child_data = {
        "STUDYID": ["S1"] * 7,
        "RDOMAIN": ["AE"] * 7,
        "USUBJID": ["U1"] * 7,
        "IDVAR": ["AESEQ", "AESEQ", "AESPID", "AESPID", "AESEQ", "AESPID", "AENOPE"],
        "IDVARVAL": ["1.0", " 2 ", "SP-3", "sp-3", "SP-3", "1", "1"],
        "QNAM": ["Q"] * 7,
        "QVAL": ["a", "b", "c", "d", "e", "f", "g"],
    }
    parent_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "USUBJID": ["U1", "U1", "U1"],
        "AESEQ": [1, 2, 3],
        "AESPID": ["01", "X", "SP-3"],
        "AETERM": ["One", "Two", "Three"],
    }

    child_schema = PostgresQLDataService.add_test_dataset(ds, "suppae", child_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", parent_data, sdtm_standards_context)

    result_schema = SqlChildMerge.perform_merge(
        pgi=ds.pgi,
        child=child_schema,
        child_domain="SUPPAE",
        datasets=ds.datasets,
        merge_spec={},
    )

    aeterm_col = result_schema.get_column_hash("aeterm")
    ds.pgi.execute_sql(f"SELECT qval, {aeterm_col} AS aeterm FROM {result_schema.hash} ORDER BY qval")
    results = [(row["qval"], row["aeterm"]) for row in ds.pgi.fetch_all()]

    assert results == [
        ("a", "One"),
        ("b", "Two"),
        ("c", "Three"),
        ("d", None),
        ("e", None),
        ("f", "One"),
        ("g", None),
    ]


def test_child_merge_parent_rows_narrowed_to_linked_ones(sdtm_standards_context):
    """
    Parent rows linked through IDVARs that few child rows use are narrowed down to the linked ones first,
    which must give the same rows: matched per subject, numerically, and once per parent row sharing a group.
    """
    ds = PostgresQLDataService.instance()

    ae_data = {
        "STUDYID": ["S1"] * 6,
        "USUBJID": ["U1", "U1", "U1", "U2", "U2", "U2"],
        "AESEQ": [1, 2, 3, 1, 2, 3],
        "AEGRPID": ["G1", "G1", "", "G1", "", ""],
    }
    fa_data = {
        "STUDYID": ["S1"] * 8,
        "USUBJID": ["U1"] * 4 + ["U2"] * 4,
        "FASEQ": [1, 2, 3, 4, 1, 2, 3, 4],
        "FAGRPID": ["G1", "G1", "", "", "G1", "", "", ""],
    }
    relrec_data = {
        "STUDYID": ["S1"] * 4,
        "USUBJID": ["U1"] * 4,
        "RDOMAIN": ["AE", "FA", "AE", "FA"],
        "IDVAR": ["AESEQ", "FASEQ", "AEGRPID", "FAGRPID"],
        "IDVARVAL": ["1.0", "2", "G1", "G1"],
        "RELID": ["R1", "R1", "R2", "R2"],
    }
    PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "fa", fa_data, sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    def merged_rows():
        result_schema = SqlChildMerge.perform_merge(
            pgi=ds.pgi, child=relrec_schema, child_domain="RELREC", datasets=ds.datasets, merge_spec={}
        )
        ds.pgi.execute_sql(
            f"""SELECT idvar, {result_schema.get_column_hash('aeseq')} AS aeseq,
                    {result_schema.get_column_hash('faseq')} AS faseq
                FROM {result_schema.hash} ORDER BY id"""
        )
        return [(row["idvar"], row["aeseq"], row["faseq"]) for row in ds.pgi.fetch_all()]

    expected = [
        ("AESEQ", 1, None),
        ("FASEQ", None, 2),
        ("AEGRPID", 1, None),
        ("AEGRPID", 2, None),
        ("FAGRPID", None, 1),
        ("FAGRPID", None, 2),
    ]
    assert merged_rows() == expected
    # the narrowed parent rows were stored, and merging again reuses them
    for parent in ("ae", "fa"):
        ds.pgi.execute_sql(
            f"SELECT to_regclass('{SqlTableSchema.derived(f'{parent}_LINKED_FROM_relrec', ds.pgi).hash}') AS oid"
        )
        assert ds.pgi.fetch_all()[0]["oid"] is not None
    assert merged_rows() == expected


def test_numeric_aware_key_agrees_with_numeric_aware_equals():
    """Keys are equal for exactly the pairs numeric_aware_equals_sql calls equal."""
    ds = PostgresQLDataService.instance()
    values = [
        "1",
        "1.0",
        "01",
        " 1 ",
        "1.",
        "1E0",
        "1e+0",
        "10E-1",
        "2",
        "-0",
        "0.000",
        "+0",
        ".5",
        "0.50",
        "5E-1",
        "-.5",
        "100",
        "1E2",
        "abc",
        "ABC",
        " abc",
        "1.0.0",
        "1E",
        "",
        None,
    ]
    rows = ", ".join("(NULL)" if value is None else f"('{value}')" for value in values)
    ds.pgi.execute_sql(
        f"""SELECT a.v AS a, b.v AS b FROM (VALUES {rows}) a(v) CROSS JOIN (VALUES {rows}) b(v)
            WHERE ({numeric_aware_key_sql("a.v")} = {numeric_aware_key_sql("b.v")})
                IS DISTINCT FROM {numeric_aware_equals_sql("a.v", "b.v")}"""
    )
    assert ds.pgi.fetch_all() == []
