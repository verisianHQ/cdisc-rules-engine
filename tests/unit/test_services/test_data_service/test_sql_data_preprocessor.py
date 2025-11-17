"""
Unit tests for SqlDataPreprocessor split dataset functionality.
"""

import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.data_service.sql_data_preprocessor import SqlDataPreprocessor
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

SIMPLE_SPLIT_AE_DATA = {
    "ae1": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["001", "002"],
        "aeseq": [1, 2],
        "aeterm": ["Headache", "Nausea"],
    },
    "ae2": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["003", "004"],
        "aeseq": [1, 2],
        "aeterm": ["Fatigue", "Dizziness"],
    },
    "ae3": {
        "studyid": ["ABC"],
        "usubjid": ["005"],
        "aeseq": [1],
        "aeterm": ["Rash"],
    },
}

SPLIT_SUPP_DATA = {
    "suppae1": {
        "studyid": ["ABC", "ABC"],
        "rdomain": ["AE", "AE"],
        "usubjid": ["001", "002"],
        "idvar": ["AESEQ", "AESEQ"],
        "idvarval": ["1", "1"],
        "qnam": ["AESPID", "AESPID"],
        "qval": ["SCREENING", "BASELINE"],
    },
    "suppae2": {
        "studyid": ["ABC"],
        "rdomain": ["AE"],
        "usubjid": ["003"],
        "idvar": ["AESEQ"],
        "idvarval": ["1"],
        "qnam": ["AESPID"],
        "qval": ["TREATMENT"],
    },
}

SPLIT_FA_DATA = {
    "facm": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["001", "002"],
        "faseq": [1, 2],
        "faobj": ["ASPIRIN", "IBUPROFEN"],
    },
    "faeg": {
        "studyid": ["ABC"],
        "usubjid": ["003"],
        "faseq": [1],
        "faobj": ["ECG NORMAL"],
    },
}

SPLIT_QS_DATA = {
    "qsa": {
        "studyid": ["ABC"],
        "usubjid": ["001"],
        "qsseq": [1],
        "qstest": ["PAIN SCALE"],
    },
    "qsb": {
        "studyid": ["ABC"],
        "usubjid": ["002"],
        "qsseq": [1],
        "qstest": ["ANXIETY SCALE"],
    },
}

NON_SPLIT_DATA = {
    "dm": {
        "studyid": ["ABC", "ABC", "ABC"],
        "usubjid": ["001", "002", "003"],
        "age": [25, 30, 35],
        "sex": ["M", "F", "M"],
    },
    "ex": {
        "studyid": ["ABC", "ABC"],
        "usubjid": ["001", "002"],
        "exseq": [1, 1],
        "extrt": ["DRUG A", "DRUG B"],
    },
}


# ============================================================================
# Split Detection Tests
# ============================================================================


def test_detect_standard_domain_splits():
    """Test detection of standard domain splits (AE1, AE2, AE3)."""
    dataset_names = ["ae1", "ae2", "ae3", "dm", "ex"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert "ae" in splits
    assert set(splits["ae"]) == {"ae1", "ae2", "ae3"}
    assert "dm" not in splits
    assert "ex" not in splits


def test_detect_supp_domain_splits():
    """Test detection of SUPP domain splits (SUPPAE1, SUPPAE2)."""
    dataset_names = ["suppae1", "suppae2", "ae", "dm"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert "suppae" in splits
    assert set(splits["suppae"]) == {"suppae1", "suppae2"}


def test_detect_fa_splits():
    """Test detection of FA (Findings About) splits by parent domain."""
    dataset_names = ["facm", "faeg", "faae", "dm"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert "fa" in splits
    assert set(splits["fa"]) == {"facm", "faeg", "faae"}


def test_detect_letter_suffix_splits():
    """Test detection of letter suffix splits (QSA, QSB)."""
    dataset_names = ["qsa", "qsb", "qsc", "dm"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert "qs" in splits
    assert set(splits["qs"]) == {"qsa", "qsb", "qsc"}


def test_detect_multiple_split_groups():
    """Test detection when multiple split groups exist."""
    dataset_names = [
        "ae1",
        "ae2",
        "ae3",
        "dm",
        "suppae1",
        "suppae2",
        "facm",
        "faeg",
        "ex1",
        "ex2",
        "qsa",
        "qsb",
    ]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert len(splits) == 5
    assert "ae" in splits
    assert "suppae" in splits
    assert "fa" in splits
    assert "ex" in splits
    assert "qs" in splits


def test_single_part_not_treated_as_split():
    """Test that single parts are filtered out (not actually split)."""
    dataset_names = ["ae", "ae1", "dm", "suppae"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert len(splits) == 0


def test_case_insensitive_detection():
    """Test that detection works regardless of case."""
    dataset_names = ["AE1", "ae2", "Ae3"]
    splits = SqlDataPreprocessor.detect_split_datasets(dataset_names)

    assert "ae" in splits
    assert len(splits["ae"]) == 3


@pytest.mark.parametrize(
    "input_name,expected_unsplit",
    [
        ("ae1", "ae"),
        ("ae2", "ae"),
        ("ae", "ae"),
        ("suppae1", "suppae"),
        ("suppae2", "suppae"),
        ("suppae", "suppae"),
        ("facm", "fa"),
        ("faeg", "fa"),
        ("fa", "fa"),
        ("suppfacm", "suppfa"),
        ("qsa", "qs"),
        ("qsb", "qs"),
        ("qs", "qs"),
        ("sqae", "sq"),
        ("dm", "dm"),
        ("ex1", "ex"),
        ("lb10", "lb"),
        ("ae123", "ae"),
    ],
)
def test_get_unsplit_name(input_name, expected_unsplit):
    """Test unsplit name extraction for various patterns."""
    actual = SqlDataPreprocessor._get_unsplit_name(input_name)
    assert actual == expected_unsplit


# ============================================================================
# Metadata Population Tests
# ============================================================================


def test_populate_metadata_simple_split():
    """Test metadata population for simple split datasets."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                    {"name": "AESEQ", "type": "integer"},
                    {"name": "AETERM", "type": "text"},
                ],
            }
        )

    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    query = """
        SELECT DISTINCT
            dataset_id,
            dataset_is_split,
            dataset_unsplit_name,
            dataset_split_part_number,
            dataset_total_split_parts
        FROM data_metadata
        WHERE dataset_id IN ('ae1', 'ae2', 'ae3')
        ORDER BY dataset_id
    """
    pgi.execute_sql(query)
    results = pgi.fetch_all()

    assert len(results) > 0

    ae1_metadata = [r for r in results if r["dataset_id"] == "ae1"]
    if ae1_metadata:
        assert ae1_metadata[0]["dataset_is_split"] is True
        assert ae1_metadata[0]["dataset_unsplit_name"] == "ae"
        assert ae1_metadata[0]["dataset_split_part_number"] == 1
        assert ae1_metadata[0]["dataset_total_split_parts"] == 3


def test_populate_metadata_supp_detection():
    """Test that SUPP datasets are correctly identified."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    PostgresQLDataService.add_test_dataset(
        data_service, "suppae1", SPLIT_SUPP_DATA["suppae1"], DefaultStandardsContext()
    )

    metadata_dicts = [
        {
            "dataset_id": "suppae1",
            "dataset_name": "SUPPAE1",
            "dataset_filename": "suppae1.xpt",
            "dataset_filepath": "/data",
            "dataset_domain": None,
            "table_hash": "suppae1",
            "variables": [{"name": "QNAM", "type": "text"}],
        }
    ]

    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    query = """
        SELECT dataset_is_supp, dataset_rdomain
        FROM data_metadata
        WHERE dataset_id = 'suppae1'
        LIMIT 1
    """
    pgi.execute_sql(query)
    result = pgi.fetch_one()

    if result:
        assert result["dataset_is_supp"] is True
        assert result["dataset_rdomain"] == "AE"


def test_populate_metadata_non_split_datasets():
    """Test that non-split datasets are marked correctly."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    PostgresQLDataService.add_test_dataset(data_service, "dm", NON_SPLIT_DATA["dm"], DefaultStandardsContext())

    metadata_dicts = [
        {
            "dataset_id": "dm",
            "dataset_name": "DM",
            "dataset_filename": "dm.xpt",
            "dataset_filepath": "/data",
            "dataset_domain": "DM",
            "table_hash": "dm",
            "variables": [{"name": "USUBJID", "type": "text"}],
        }
    ]

    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    query = """
        SELECT dataset_is_split, dataset_unsplit_name
        FROM data_metadata
        WHERE dataset_id = 'dm'
        LIMIT 1
    """
    pgi.execute_sql(query)
    result = pgi.fetch_one()

    if result:
        assert result["dataset_is_split"] is False
        assert result["dataset_unsplit_name"] == "dm"


# ============================================================================
# Concatenation Tests
# ============================================================================


def test_concatenate_simple_splits():
    """Test concatenation of simple split datasets."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                    {"name": "AESEQ", "type": "integer"},
                    {"name": "AETERM", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1
    assert results["total_parts_concatenated"] == 3
    assert results["source_tracking_added"] is True

    check_table_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'ae'
        )
    """
    pgi.execute_sql(check_table_query)
    table_exists = pgi.fetch_one()["exists"]
    assert table_exists is True

    check_column_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'ae'
            AND column_name = '_source_ds'
        )
    """
    pgi.execute_sql(check_column_query)
    column_exists = pgi.fetch_one()["exists"]
    assert column_exists is True

    count_query = "SELECT COUNT(*) as count FROM ae"
    pgi.execute_sql(count_query)
    row_count = pgi.fetch_one()["count"]
    expected_count = sum(len(data["usubjid"]) for data in SIMPLE_SPLIT_AE_DATA.values())
    assert row_count == expected_count


def test_source_ds_column_values():
    """Test that _SOURCE_DS column contains correct source dataset names."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    preprocessor._process_split_datasets()

    query = """
        SELECT DISTINCT _source_ds
        FROM ae
        ORDER BY _source_ds
    """
    pgi.execute_sql(query)
    sources = pgi.fetch_all()
    source_values = {row["_source_ds"] for row in sources}

    assert source_values == {"AE1", "AE2", "AE3"}


def test_concatenate_supp_splits():
    """Test concatenation of SUPP split datasets."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SPLIT_SUPP_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SPLIT_SUPP_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": None,
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                    {"name": "QNAM", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    check_query = "SELECT COUNT(*) as count FROM suppae"
    pgi.execute_sql(check_query)
    count = pgi.fetch_one()["count"]
    expected = sum(len(data["usubjid"]) for data in SPLIT_SUPP_DATA.values())
    assert count == expected


def test_concatenate_fa_splits():
    """Test concatenation of FA (Findings About) splits."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SPLIT_FA_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SPLIT_FA_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "FA",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'fa'
        )
    """
    pgi.execute_sql(check_query)
    exists = pgi.fetch_one()["exists"]
    assert exists is True


def test_concatenate_letter_suffix_splits():
    """Test concatenation of letter suffix splits (questionnaires)."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SPLIT_QS_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SPLIT_QS_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "QS",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    query = "SELECT DISTINCT _source_ds FROM qs ORDER BY _source_ds"
    pgi.execute_sql(query)
    sources = {row["_source_ds"] for row in pgi.fetch_all()}
    assert sources == {"QSA", "QSB"}


def test_multiple_split_groups_simultaneously():
    """Test processing multiple split groups in one preprocessing run."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    all_data = {**SIMPLE_SPLIT_AE_DATA, **SPLIT_SUPP_DATA}

    for table_name, data in all_data.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in all_data.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE" if "ae" in table_name else None,
                "table_hash": table_name,
                "variables": [{"name": "STUDYID", "type": "text"}],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 2

    for table in ["ae", "suppae"]:
        check_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '{table}'
            )
        """
        pgi.execute_sql(check_query)
        exists = pgi.fetch_one()["exists"]
        assert exists is True, f"Table {table} should exist"


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_empty_dataset_list():
    """Test handling of empty dataset list."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, [])

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 0


def test_no_split_datasets():
    """Test preprocessing when no split datasets exist."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in NON_SPLIT_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in NON_SPLIT_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": table_name.upper(),
                "table_hash": table_name,
                "variables": [{"name": "USUBJID", "type": "text"}],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 0
    assert results["total_parts_concatenated"] == 0


# ============================================================================
# Integration Tests
# ============================================================================


def test_full_preprocessing_pipeline():
    """Test the complete preprocessing pipeline."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    results = preprocessor.preprocess_all()

    assert "split_processing" in results
    assert "relrec_catalog" in results
    assert "co_catalog" in results
    assert "supp_catalog" in results
    assert "validation_errors" in results
    assert "metadata_updates" in results
    assert "run_id" in results
    assert "timestamp" in results

    assert results["split_processing"]["groups_processed"] == 1


def test_query_concatenated_dataset():
    """Test querying a concatenated dataset."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [
                    {"name": "STUDYID", "type": "text"},
                    {"name": "USUBJID", "type": "text"},
                    {"name": "AETERM", "type": "text"},
                ],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    preprocessor._process_split_datasets()

    query = """
        SELECT usubjid, aeterm, _source_ds
        FROM ae
        WHERE usubjid = '001'
    """
    pgi.execute_sql(query)
    results = pgi.fetch_all()

    assert len(results) > 0
    result = results[0]
    assert result["usubjid"] == "001"
    assert result["aeterm"] == "Headache"
    assert result["_source_ds"] == "AE1"


def test_filter_by_source_ds():
    """Test filtering concatenated dataset by _SOURCE_DS."""
    data_service = PostgresQLDataService.instance()
    pgi = data_service.pgi

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, DefaultStandardsContext())

    metadata_dicts = []
    for table_name in SIMPLE_SPLIT_AE_DATA.keys():
        metadata_dicts.append(
            {
                "dataset_id": table_name,
                "dataset_name": table_name.upper(),
                "dataset_filename": f"{table_name}.xpt",
                "dataset_filepath": "/data",
                "dataset_domain": "AE",
                "table_hash": table_name,
                "variables": [{"name": "STUDYID", "type": "text"}],
            }
        )
    SqlDataPreprocessor.populate_metadata_for_datasets(pgi, metadata_dicts)

    preprocessor = SqlDataPreprocessor(pgi)
    preprocessor._process_split_datasets()

    query = """
        SELECT COUNT(*) as count
        FROM ae
        WHERE _source_ds = 'AE1'
    """
    pgi.execute_sql(query)
    result = pgi.fetch_one()

    expected = len(SIMPLE_SPLIT_AE_DATA["ae1"]["usubjid"])
    assert result["count"] == expected
