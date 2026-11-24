from contextlib import contextmanager

import pytest

from cdisc_rules_engine.data_service.merges.relrec import LINK_PLANNER_SETTINGS, SqlRelrecMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError

SIMPLE_RELREC_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 2, 1],
        "ECSTDY": [1, 5, 10],
    },
    "relrec": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "RELID": ["REL001", "REL001"],
        "RDOMAIN": ["EC", "AE"],
        "IDVAR": ["ECSEQ", "AESEQ"],
        "IDVARVAL": ["1", "1"],
    },
    "ae": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [2],
    },
}

COMPLEX_RELREC_DATA = {
    "original": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "ECSEQ": [1, 2, 1, 2],
        "ECSTDY": [1, 5, 10, 15],
    },
    "relrec": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "RELID": ["REL001", "REL001", "REL002", "REL002"],
        "RDOMAIN": ["EC", "AE", "EC", "DM"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", ""],
        "IDVARVAL": ["1", "1", "1", ""],
    },
    "ae": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [2],
    },
    "dm": {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ002"],
        "AGE": [30],
    },
}


@pytest.mark.parametrize(
    "data, expected_columns",
    [
        (
            SIMPLE_RELREC_DATA,
            [
                "studyid",
                "usubjid",
                "ecseq",
                "ecstdy",
                "relrec.studyid",
                "relrec.usubjid",
                "relrec.__seq",
                "relrec.__stdy",
            ],
        ),
        (
            COMPLEX_RELREC_DATA,
            [
                "studyid",
                "usubjid",
                "ecseq",
                "ecstdy",
                "relrec.studyid",
                "relrec.usubjid",
                "relrec.__seq",
                "relrec.__stdy",
                "relrec.age",
            ],
        ),
    ],
)
def test_relrec_merge_column_structure(data, expected_columns, sdtm_standards_context):
    """Test that RELREC merge creates the correct column structure."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create and populate original table
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)

    # Create and populate RELREC table
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    # Create related domain tables
    for domain, domain_data in data.items():
        if domain not in ["original", "relrec"]:
            PostgresQLDataService.add_test_dataset(ds, domain, domain_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Check that expected columns are present
    for expected_col in expected_columns:
        assert result_schema.has_column(expected_col), f"Missing expected column: {expected_col}"


def test_relrec_merge_basic_functionality(sdtm_standards_context):
    """Test basic RELREC merge functionality with simple data."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi
    data = SIMPLE_RELREC_DATA

    # Create and populate tables
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    PostgresQLDataService.add_test_dataset(ds, "ae", data["ae"], sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Check that result table was created and has data
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    assert result[0]["count"] >= 1, "Result table should have at least one row"


def test_relrec_merge_no_relationships(sdtm_standards_context):
    """Test RELREC merge when no relationships exist for the domain."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create original data
    original_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "ECSEQ": [1],
        "ECSTDY": [1],
    }
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", original_data, sdtm_standards_context)

    # Create RELREC with no EC relationships
    relrec_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "RELID": ["REL001"],
        "RDOMAIN": ["AE"],  # No EC domain
        "IDVAR": ["AESEQ"],
        "IDVARVAL": ["1"],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    # Should have same data as original since no relationships
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    assert result[0]["count"] == 1, "Should have original data when no relationships exist"


def test_relrec_merge_wildcard_renaming(sdtm_standards_context):
    """Test that wildcard renaming works correctly."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi
    data = SIMPLE_RELREC_DATA

    # Create tables
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)

    PostgresQLDataService.add_test_dataset(ds, "ae", data["ae"], sdtm_standards_context)

    # Test different wildcard values
    for wildcard in ["__", "XX", "**"]:
        result_schema = SqlRelrecMerge.perform_join(
            pgi=pgi,
            original=original_schema,
            relrec=relrec_schema,
            domain="EC",
            wildcard=wildcard,
        )

        # Check that wildcard is correctly applied (note: column names are lowercase in PostgreSQL)
        expected_col = f"relrec.{wildcard.lower()}stdy"
        assert result_schema.has_column(expected_col), f"Missing wildcard column: {expected_col}"


def test_relrec_validation_errors(sdtm_standards_context):
    """Test that validation errors are properly raised."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create table without required columns
    invalid_original_data = {
        "DOMAIN": ["EC"],  # Missing STUDYID, USUBJID
        "ECSEQ": [1],
    }
    invalid_original_schema = PostgresQLDataService.add_test_dataset(
        ds, "ec_invalid", invalid_original_data, sdtm_standards_context
    )

    relrec_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "RELID": ["REL001"],
        "RDOMAIN": ["EC"],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Should raise validation error
    with pytest.raises(ColumnNotFoundError, match="Column 'STUDYID' not found in table 'ec_invalid'"):
        SqlRelrecMerge.perform_join(
            pgi=pgi,
            original=invalid_original_schema,
            relrec=relrec_schema,
            domain="EC",
            wildcard="__",
        )


def test_relrec_relationship_kinds(sdtm_standards_context):
    """Test the _relationship_kinds method."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002", "SUBJ002"],
        "RELID": ["REL001", "REL001", "REL002", "REL002"],
        "RDOMAIN": ["EC", "AE", "EC", "DM"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", ""],
        "IDVARVAL": ["1", "1", "1", ""],
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Filter for EC domain
    relationships = SqlRelrecMerge._relationship_kinds(pgi, relrec_schema, "EC")

    # Should return relationships where left side is EC and right side is not EC
    assert len(relationships) == 2, "Should find 2 relationships for EC domain"

    # Check that we have the expected relationships
    right_domains = [rel["rdomain_right"] for rel in relationships]
    assert "AE" in right_domains, "Should include AE relationship"
    assert "DM" in right_domains, "Should include DM relationship"
    assert "EC" not in right_domains, "Should not include EC as right domain"


def test_relrec_apply_wildcard_renaming(sdtm_standards_context):
    """Test the _apply_wildcard_renaming method."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create a test domain table
    domain_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [1],
        "AETERM": ["Headache"],
    }
    domain_schema = PostgresQLDataService.add_test_dataset(ds, "ae", domain_data, sdtm_standards_context)

    # Test wildcard renaming
    renamed_columns = SqlRelrecMerge._apply_wildcard_renaming(pgi, domain_schema, "AE", "__")

    # Check expected renamings (note: column names are lowercase in PostgreSQL)
    assert "aestdy" in renamed_columns, "AESTDY should be in renamed columns"
    assert renamed_columns["aestdy"] == "RELREC.__STDY", "AESTDY should be renamed to RELREC.__STDY"

    assert "aeseq" in renamed_columns, "AESEQ should be in renamed columns"
    assert renamed_columns["aeseq"] == "RELREC.__SEQ", "AESEQ should be renamed to RELREC.__SEQ"

    assert "aeterm" in renamed_columns, "AETERM should be in renamed columns"
    assert renamed_columns["aeterm"] == "RELREC.aeterm", "AETERM should keep original name with RELREC prefix"


def test_relrec_apply_wildcard_renaming_with_model_variables(sdtm_standards_context):
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    domain_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "AESEQ": [1],
        "AESTDY": [1],
        "AETERM": ["Headache"],
        "AEFOO": ["sponsor-defined"],
    }
    domain_schema = PostgresQLDataService.add_test_dataset(ds, "ae", domain_data, sdtm_standards_context)

    model_wildcard_variables = {"--SEQ", "--TERM"}

    renamed_columns = SqlRelrecMerge._apply_wildcard_renaming(pgi, domain_schema, "AE", "__", model_wildcard_variables)

    assert renamed_columns["aeterm"] == "RELREC.__TERM", "AETERM should be wildcarded from the model"
    assert renamed_columns["aeseq"] == "RELREC.__SEQ", "AESEQ should be wildcarded from the model"
    assert renamed_columns["aestdy"] == "RELREC.aestdy", "AESTDY is not a model wildcard variable here"
    assert renamed_columns["aefoo"] == "RELREC.aefoo", "Sponsor-defined AEFOO should keep its own name"

    assert renamed_columns["usubjid"] == "RELREC.usubjid"


def test_relrec_merge_cg0601_scenario(sdtm_standards_context):
    """Test the specific CG0601 scenario with data type mismatch (string vs integer)."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create AE data matching CG0601 test case
    ae_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001", "CDISC001"],
        "AESEQ": [1, 2, 3],
        "AEGRPID": ["1", "2", "3"],  # String values like in Excel
        "AETERM": ["HEADACHE", "FATIGUE", "NAUSEA"],
        "AEDECOD": ["Headache", None, "Nausea"],  # Row 2 has null AEDECOD
    }
    ae_schema = PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)

    # Create FA data matching CG0601 test case
    fa_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001", "CDISC001"],
        "FASEQ": [1, 2, 3],
        "FAGRPID": [1, 2, 3],  # Integer values like in Excel
        "FAOBJ": ["OK", "ERROR", "WARNING"],  # Row 2 has "ERROR"
        "FATEST": ["Severity", "Severity", "Severity"],
    }
    PostgresQLDataService.add_test_dataset(ds, "fa", fa_data, sdtm_standards_context)

    # Create RELREC data matching CG0601 test case
    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["CDISC001", "CDISC001"],
        "RELID": ["AEFA", "AEFA"],
        "RDOMAIN": ["AE", "FA"],
        "IDVAR": ["AEGRPID", "FAGRPID"],
        "IDVARVAL": ["", ""],  # Empty values - should join on IDVAR columns directly
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=ae_schema,
        relrec=relrec_schema,
        domain="AE",
        wildcard="FA",
    )

    # Check that the merge created the expected columns
    assert result_schema.has_column("relrec.faobj"), "Should have RELREC.FAOBJ column"

    # Check that the data was joined correctly
    pgi.execute_sql(
        f"""
        SELECT
            {ae_schema.get_column_hash('USUBJID')} as usubjid,
            {ae_schema.get_column_hash('AESEQ')} as aeseq,
            {ae_schema.get_column_hash('AETERM')} as aeterm,
            {ae_schema.get_column_hash('AEDECOD')} as aedecod,
            {result_schema.get_column_hash('relrec.faobj')} as faobj
        FROM {result_schema.hash}
        WHERE {ae_schema.get_column_hash('AESEQ')} = 2
    """
    )
    result = pgi.fetch_all()

    # Should find the relationship: AESEQ=2, AETERM="FATIGUE", AEDECOD=null, FAOBJ="ERROR"
    assert len(result) == 1, f"Should find exactly 1 record for AESEQ=2, got {len(result)}"

    record = result[0]
    assert record["usubjid"] == "CDISC001", f"Expected USUBJID=CDISC001, got {record['usubjid']}"
    assert record["aeseq"] == 2, f"Expected AESEQ=2, got {record['aeseq']}"
    assert record["aeterm"] == "FATIGUE", f"Expected AETERM=FATIGUE, got {record['aeterm']}"
    assert record["aedecod"] is None, f"Expected AEDECOD=null, got {record['aedecod']}"
    assert record["faobj"] == "ERROR", f"Expected FAOBJ=ERROR, got {record['faobj']}"


def test_relrec_merge_multiple_relationships_no_duplicates(sdtm_standards_context):
    """Test that multiple relationships for same record don't create duplicates."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    # Create original data with one record
    original_data = {
        "STUDYID": ["STUDY001"],
        "USUBJID": ["SUBJ001"],
        "FASEQ": [1],
        "FAGRPID": ["1"],
        "FADECOD": ["TEST"],
    }
    original_schema = PostgresQLDataService.add_test_dataset(ds, "fa", original_data, sdtm_standards_context)

    # Create right domain data with multiple related records
    right_data = {
        "STUDYID": ["STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "CMSEQ": [1, 2],
        "CMGRPID": ["1", "1"],  # Both relate to same FA record
        "FAOBJ": ["VALUE1", "VALUE2"],
    }
    PostgresQLDataService.add_test_dataset(ds, "cm", right_data, sdtm_standards_context)

    # Create RELREC data with multiple relationships to same FA record
    relrec_data = {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001", "STUDY001"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ001", "SUBJ001"],
        "RELID": ["REL1", "REL1", "REL2", "REL2"],
        "RDOMAIN": ["FA", "CM", "FA", "CM"],
        "IDVAR": ["FAGRPID", "CMGRPID", "FAGRPID", "CMGRPID"],
        "IDVARVAL": ["1", "1", "1", "1"],  # Specific values - should use WHERE filtering
    }
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    # Perform RELREC merge
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="FA",
        wildcard="CM",
    )

    # Count total records in result
    pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result_schema.hash}")
    result = pgi.fetch_all()
    total_count = result[0]["count"]

    # Should have exactly 2 records (one for each CM relationship), not 4
    # The UNION should eliminate any duplicates from multiple RELREC relationships
    assert total_count == 2, f"Expected 2 records (no duplicates), got {total_count}"

    # Verify we have the expected FAOBJ values
    pgi.execute_sql(
        f"""
        SELECT DISTINCT {result_schema.get_column_hash('relrec.faobj')} as faobj
        FROM {result_schema.hash}
        ORDER BY faobj
    """
    )
    faobj_results = pgi.fetch_all()
    faobj_values = [r["faobj"] for r in faobj_results]

    assert len(faobj_values) == 2, f"Expected 2 distinct FAOBJ values, got {len(faobj_values)}"
    assert "VALUE1" in faobj_values, "Should have VALUE1"
    assert "VALUE2" in faobj_values, "Should have VALUE2"


def test_relrec_merge_idvarval_scientific_notation(sdtm_standards_context):
    """Test that RELREC join correctly handles scientific notation"""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    original_data = {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "ECSEQ": [1000000000000000, 100000],
        "ECSTDY": [1, 2],
    }
    ae_data = {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U2"],
        "AESEQ": [1000000000000000, 100000],
        "AETERM": ["Big", "Sci"],
    }
    relrec_data = {
        "STUDYID": ["S1", "S1", "S1", "S1"],
        "USUBJID": ["U1", "U1", "U2", "U2"],
        "RELID": ["REL1", "REL1", "REL2", "REL2"],
        "RDOMAIN": ["EC", "AE", "EC", "AE"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", "AESEQ"],
        "IDVARVAL": ["1000000000000000", "1000000000000000", "1E5", "1E5"],
    }

    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", original_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)
    assert original_schema.get_column("ecseq").type == "Num"

    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi,
        original=original_schema,
        relrec=relrec_schema,
        domain="EC",
        wildcard="__",
    )

    assert result_schema.has_column("relrec.aeterm")
    pgi.execute_sql(
        f"""SELECT usubjid, {result_schema.get_column_hash('relrec.aeterm')} as aeterm
            FROM {result_schema.hash} ORDER BY usubjid"""
    )
    results = {r["usubjid"]: r["aeterm"] for r in pgi.fetch_all()}

    assert results["U1"] == "Big"
    assert results["U2"] == "Sci"


def test_relrec_merge_dataset_level_blank_group_ids(sdtm_standards_context):
    """A dataset-level relationship links records by --GRPID, and records with a blank --GRPID aren't linked."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    ae_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "USUBJID": ["U1", "U1", "U1"],
        "AESEQ": [1, 2, 3],
        "AEGRPID": ["G1", "", ""],
        "AETERM": ["HEADACHE", "FATIGUE", "NAUSEA"],
    }
    fa_data = {
        "STUDYID": ["S1", "S1", "S1"],
        "USUBJID": ["U1", "U1", "U1"],
        "FASEQ": [1, 2, 3],
        "FAGRPID": ["G1", "", ""],
        "FAOBJ": ["HEADACHE", "OTHER", "OTHER"],
    }
    relrec_data = {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["", ""],
        "RELID": ["AEFA", "AEFA"],
        "RDOMAIN": ["AE", "FA"],
        "IDVAR": ["AEGRPID", "FAGRPID"],
        "IDVARVAL": ["", ""],
    }
    ae_schema = PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "fa", fa_data, sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi, original=ae_schema, relrec=relrec_schema, domain="AE", wildcard="FA"
    )

    pgi.execute_sql(
        f"""SELECT {ae_schema.get_column_hash('AESEQ')} AS aeseq,
                {result_schema.get_column_hash('relrec.faseq')} AS faseq
            FROM {result_schema.hash}"""
    )
    assert [(r["aeseq"], r["faseq"]) for r in pgi.fetch_all()] == [(1, 1)]


def test_relrec_merge_pairs_linking_same_records_give_one_row(sdtm_standards_context):
    """Pairs of different kinds that link the same two records give one row."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    ec_data = {"STUDYID": ["S1"], "USUBJID": ["U1"], "ECSEQ": [1], "ECGRPID": ["G1"]}
    ae_data = {
        "STUDYID": ["S1", "S1"],
        "USUBJID": ["U1", "U1"],
        "AESEQ": [1, 2],
        "AEGRPID": ["G1", "G1"],
        "AETERM": ["HEADACHE", "HEADACHE"],
    }
    relrec_data = {
        "STUDYID": ["S1"] * 4,
        "USUBJID": ["U1"] * 4,
        "RELID": ["R1", "R1", "R2", "R2"],
        "RDOMAIN": ["EC", "AE", "EC", "AE"],
        "IDVAR": ["ECSEQ", "AESEQ", "ECGRPID", "AEGRPID"],
        "IDVARVAL": ["1", "1", "G1", "G1"],
    }
    ec_schema = PostgresQLDataService.add_test_dataset(ds, "ec", ec_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi, original=ec_schema, relrec=relrec_schema, domain="EC", wildcard="__"
    )

    # R1 links ECSEQ 1 to AESEQ 1, R2 links group G1 (ECSEQ 1) to group G1 (AESEQ 1 and 2)
    pgi.execute_sql(
        f"""SELECT {result_schema.get_column_hash('relrec.__seq')} AS aeseq,
                {result_schema.get_column_hash('relrec.aeterm')} AS aeterm
            FROM {result_schema.hash} ORDER BY id"""
    )
    assert [(r["aeseq"], r["aeterm"]) for r in pgi.fetch_all()] == [(1, "HEADACHE"), (2, "HEADACHE")]


def test_relrec_merge_idvar_naming_missing_variable_links_nothing(sdtm_standards_context):
    """A pair whose IDVAR names a variable that isn't in the dataset links nothing, on either side."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi

    ec_data = {"STUDYID": ["S1", "S1"], "USUBJID": ["U1", "U1"], "ECSEQ": [1, 2]}
    ae_data = {"STUDYID": ["S1", "S1"], "USUBJID": ["U1", "U1"], "AESEQ": [1, 2]}
    relrec_data = {
        "STUDYID": ["S1"] * 6,
        "USUBJID": ["U1"] * 6,
        "RELID": ["R1", "R1", "R2", "R2", "R3", "R3"],
        "RDOMAIN": ["EC", "AE"] * 3,
        "IDVAR": ["ECSEQ", "AESEQ", "ECSEQ", "AESEQX", "ECSEQX", "AESEQ"],
        "IDVARVAL": ["1", "1", "2", "2", "1", "2"],
    }
    ec_schema = PostgresQLDataService.add_test_dataset(ds, "ec", ec_data, sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", ae_data, sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", relrec_data, sdtm_standards_context)

    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi, original=ec_schema, relrec=relrec_schema, domain="EC", wildcard="__"
    )

    # only R1 links records: R2 names no AE variable (AESEQX), R3 no EC one (ECSEQX)
    pgi.execute_sql(
        f"""SELECT {ec_schema.get_column_hash('ECSEQ')} AS ecseq,
                {result_schema.get_column_hash('relrec.__seq')} AS aeseq
            FROM {result_schema.hash} ORDER BY id"""
    )
    assert [(r["ecseq"], r["aeseq"]) for r in pgi.fetch_all()] == [(1, 1)]


def test_relrec_merge_leaves_connections_as_they_were(sdtm_standards_context, monkeypatch):
    """The merge changes planner settings and creates tables for its own transaction only."""
    ds = PostgresQLDataService.instance()
    pgi = ds.pgi
    data = SIMPLE_RELREC_DATA
    original_schema = PostgresQLDataService.add_test_dataset(ds, "ec", data["original"], sdtm_standards_context)
    relrec_schema = PostgresQLDataService.add_test_dataset(ds, "relrec", data["relrec"], sdtm_standards_context)
    PostgresQLDataService.add_test_dataset(ds, "ae", data["ae"], sdtm_standards_context)

    def show_settings(connection):
        with connection.cursor() as cursor:
            values = {}
            for name in LINK_PLANNER_SETTINGS:
                cursor.execute(f"SHOW {name}")
                values[name] = cursor.fetchone()[0]
        connection.rollback()
        return values

    get_connection_and_cursor = pgi.db.get_connection_and_cursor
    with get_connection_and_cursor() as (connection, _):
        settings_before = show_settings(connection)

    # the connections the merge uses
    connections = []

    @contextmanager
    def recording_connection_and_cursor(*args, **kwargs):
        with get_connection_and_cursor(*args, **kwargs) as (connection, cursor):
            connections.append(connection)
            yield connection, cursor

    monkeypatch.setattr(pgi.db, "get_connection_and_cursor", recording_connection_and_cursor)
    result_schema = SqlRelrecMerge.perform_join(
        pgi=pgi, original=original_schema, relrec=relrec_schema, domain="EC", wildcard="__"
    )
    monkeypatch.undo()

    assert connections
    for connection in set(connections):
        assert show_settings(connection) == settings_before
        with connection.cursor() as cursor:
            for table in (f"pairs_{result_schema.hash}", f"links_{result_schema.hash}"):
                cursor.execute("SELECT to_regclass(%s) AS oid", (table,))
                assert cursor.fetchone()[0] is None, f"{table} outlived the merge"
        connection.rollback()
