"""
Unit tests for SqlDataPreprocessor split dataset functionality.
"""

import re
from collections import defaultdict
from typing import Dict, List

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


class TestSdtmStandardsContext(DefaultStandardsContext):
    """
    Test SDTM standards context with split detection implementation
    without requiring library metadata args.
    """

    def detect_split_datasets(self, dataset_names: List[str]) -> Dict[str, List[str]]:
        """Detect split datasets by naming convention."""
        split_groups = defaultdict(list)

        datasets = [name.lower() for name in dataset_names]

        for dataset in datasets:
            unsplit_name = self._get_unsplit_name(dataset)

            if unsplit_name != dataset:
                split_groups[unsplit_name].append(dataset)

        return {k: v for k, v in split_groups.items() if len(v) > 1}

    @staticmethod
    def _get_unsplit_name(dataset_name: str) -> str:
        """Extract the unsplit (logical) name from a dataset name."""
        dataset = dataset_name.lower()

        # Pattern 1: Domain + digit(s) (e.g., AE1, AE2, QS36)
        match = re.match(r"^([a-z]{2,4})(\d+)$", dataset)
        if match:
            return match.group(1)

        # Pattern 2: SUPP + domain + digit (e.g., SUPPAE1, SUPPQS2)
        match = re.match(r"^supp([a-z]{2,4})(\d+)$", dataset)
        if match:
            return f"supp{match.group(1)}"

        # Pattern 3: FA + 2-char parent domain (e.g., FACM, FAEG)
        match = re.match(r"^fa([a-z]{2})$", dataset)
        if match:
            return "fa"

        # Pattern 4: SUPP + FA + parent domain (e.g., SUPPFACM, SUPPFAEG)
        match = re.match(r"^suppfa([a-z]{2})$", dataset)
        if match:
            return "suppfa"

        # Pattern 5: SQ (Supplemental Qualifiers) + suffix
        match = re.match(r"^sq([a-z]+\d*)$", dataset)
        if match:
            return "sq"

        # Pattern 6: Domain + letter suffix (e.g., QSA, QSB for questionnaires)
        match = re.match(r"^([a-z]{2,4})([a-z])$", dataset)
        if match:
            base = match.group(1)
            suffix = match.group(2)
            if len(base) >= 2 and len(suffix) == 1:
                return base

        return dataset


# ============================================================================
# Concatenation Tests
# ============================================================================


def test_concatenate_simple_splits():
    """Test concatenation of simple split datasets."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    initial_count = len(data_service.datasets)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1
    assert results["total_parts_concatenated"] == 3

    assert len(data_service.datasets) == initial_count + 1

    check_table_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'ae'
        )
    """
    data_service.pgi.execute_sql(check_table_query)
    table_exists = data_service.pgi.fetch_one()["exists"]
    assert table_exists is True

    check_column_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'ae'
            AND column_name = 'source_ds'
        )
    """
    data_service.pgi.execute_sql(check_column_query)
    column_exists = data_service.pgi.fetch_one()["exists"]
    assert column_exists is True

    count_query = "SELECT COUNT(*) as count FROM ae"
    data_service.pgi.execute_sql(count_query)
    row_count = data_service.pgi.fetch_one()["count"]
    expected_count = sum(len(data["usubjid"]) for data in SIMPLE_SPLIT_AE_DATA.values())
    assert row_count == expected_count


def test_source_ds_column_values():
    """Test that SOURCE_DS column contains correct source dataset names."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    query = """
        SELECT DISTINCT source_ds
        FROM ae
        ORDER BY source_ds
    """
    data_service.pgi.execute_sql(query)
    sources = data_service.pgi.fetch_all()
    source_values = {row["source_ds"] for row in sources}

    assert source_values == {"AE1", "AE2", "AE3"}


def test_concatenate_supp_splits():
    """Test concatenation of SUPP split datasets."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SPLIT_SUPP_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    check_query = "SELECT COUNT(*) as count FROM suppae"
    data_service.pgi.execute_sql(check_query)
    count = data_service.pgi.fetch_one()["count"]
    expected = sum(len(data["usubjid"]) for data in SPLIT_SUPP_DATA.values())
    assert count == expected


def test_concatenate_fa_splits():
    """Test concatenation of FA (Findings About) splits."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SPLIT_FA_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'fa'
        )
    """
    data_service.pgi.execute_sql(check_query)
    exists = data_service.pgi.fetch_one()["exists"]
    assert exists is True


def test_concatenate_letter_suffix_splits():
    """Test concatenation of letter suffix splits (questionnaires)."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SPLIT_QS_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 1

    query = "SELECT DISTINCT source_ds FROM qs ORDER BY source_ds"
    data_service.pgi.execute_sql(query)
    sources = {row["source_ds"] for row in data_service.pgi.fetch_all()}
    assert sources == {"QSA", "QSB"}


def test_multiple_split_groups_simultaneously():
    """Test processing multiple split groups in one preprocessing run."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    all_data = {**SIMPLE_SPLIT_AE_DATA, **SPLIT_SUPP_DATA}

    for table_name, data in all_data.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 2

    for table in ["ae", "suppae"]:
        check_query = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '{table}'
            )
        """
        data_service.pgi.execute_sql(check_query)
        exists = data_service.pgi.fetch_one()["exists"]
        assert exists is True, f"Table {table} should exist"


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_no_split_datasets():
    """Test preprocessing when no split datasets exist."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in NON_SPLIT_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 0
    assert results["total_parts_concatenated"] == 0


def test_empty_datasets_list():
    """Test preprocessing with no datasets loaded."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    data_service.datasets = []

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    results = preprocessor._process_split_datasets()

    assert results["groups_processed"] == 0
    assert results["total_parts_concatenated"] == 0


# ============================================================================
# Integration Tests
# ============================================================================


def test_full_preprocessing_pipeline():
    """Test the complete preprocessing pipeline."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
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
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    query = """
        SELECT usubjid, aeterm, source_ds
        FROM ae
        WHERE usubjid = '001'
    """
    data_service.pgi.execute_sql(query)
    results = data_service.pgi.fetch_all()

    assert len(results) > 0
    result = results[0]
    assert result["usubjid"] == "001"
    assert result["aeterm"] == "Headache"
    assert result["source_ds"] == "AE1"


def test_filter_by_source_ds():
    """Test filtering concatenated dataset by SOURCE_DS."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    query = """
        SELECT COUNT(*) as count
        FROM ae
        WHERE source_ds = 'AE1'
    """
    data_service.pgi.execute_sql(query)
    result = data_service.pgi.fetch_one()

    expected = len(SIMPLE_SPLIT_AE_DATA["ae1"]["usubjid"])
    assert result["count"] == expected


def test_metadata_created_for_concatenated_datasets():
    """Test that metadata is properly created for concatenated datasets."""
    data_service = PostgresQLDataService.instance()
    standards_context = TestSdtmStandardsContext()

    for table_name, data in SIMPLE_SPLIT_AE_DATA.items():
        PostgresQLDataService.add_test_dataset(data_service, table_name, data, standards_context)

    initial_count = len(data_service.datasets)

    preprocessor = SqlDataPreprocessor(data_service, standards_context)
    preprocessor._process_split_datasets()

    assert len(data_service.datasets) == initial_count + 1

    ae_metadata = next((ds for ds in data_service.datasets if ds.name.upper() == "AE"), None)
    assert ae_metadata is not None
    assert ae_metadata.filename == "ae.xpt"
