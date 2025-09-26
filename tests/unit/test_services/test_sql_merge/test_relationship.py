import pytest

from cdisc_rules_engine.data_service.merges.relationship import SqlRelationshipMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


# Test data for relationship merge scenarios
SIMPLE_RELATIONSHIP_DATA = {
    "ec": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "DOMAIN": ["EC", "EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 2, 1],
        "ECSTDY": [1, 5, 10],
    },
    "relsub": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "RDOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ001"],
        "IDVAR": ["ECSEQ", "ECSEQ"],
        "IDVARVAL": ["1", "2"],
        "RSUBJID": ["SUBJ001", "SUBJ001"],
        "POOLID": ["POOL1", "POOL2"],
    },
}

EMPTY_RELATIONSHIP_DATA = {
    "ec": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "DOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ002"],
        "ECSEQ": [1, 1],
        "ECSTDY": [1, 10],
    },
    "relsub": {
        "STUDYID": ["STUDY001", "STUDY001"],
        "RDOMAIN": ["EC", "EC"],
        "USUBJID": ["SUBJ001", "SUBJ002"],
        "IDVAR": ["", ""],
        "IDVARVAL": ["", ""],
        "RSUBJID": ["SUBJ001", "SUBJ002"],
    },
}

COMPLEX_RELATIONSHIP_DATA = {
    "ae": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "DOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "AESEQ": [1, 2, 1],
        "AESTDY": [2, 7, 12],
        "AESEV": ["MILD", "SEVERE", "MODERATE"],
    },
    "co": {
        "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
        "RDOMAIN": ["AE", "AE", "AE"],
        "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ002"],
        "IDVAR": ["AESEQ", "AESEV", "AESEQ"],
        "IDVARVAL": ["1", "MILD", "1"],
        "COREFID": ["REF001", "REF002", "REF003"],
        "COREF": ["Reference comment 1", "Severity comment", "Reference comment 3"],
    },
}


class TestSqlRelationshipMerge:
    """Test cases for SqlRelationshipMerge functionality."""

    def _load_test_data(self, data_service, test_data):
        """Helper method to load test data into the database."""
        loaded_schemas = {}
        for table_name, data in test_data.items():
            schema = PostgresQLDataService.add_test_dataset(data_service, table_name, data)
            loaded_schemas[table_name] = schema
        return loaded_schemas

    def test_simple_relationship_merge(self):
        """Test basic relationship merge functionality."""
        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, SIMPLE_RELATIONSHIP_DATA)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        result = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ec"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Verify the result schema
        assert result is not None
        assert result.has_column("STUDYID")
        assert result.has_column("USUBJID")
        assert result.has_column("ECSEQ")
        assert result.has_column("ECSTDY")
        assert result.has_column("RDOMAIN.RELSUB")
        assert result.has_column("POOLID.RELSUB")

        # Verify data was merged correctly
        data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result.hash}")
        row_count = data_service.pgi.fetch_one()["count"]
        assert row_count > 0

    def test_empty_relationship_columns_merge(self):
        """Test relationship merge when relationship columns are empty."""
        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, EMPTY_RELATIONSHIP_DATA)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        result = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ec"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Should perform simple outer join when relationship columns are empty
        assert result is not None
        assert result.has_column("RSUBJID.RELSUB")

        # Verify data was merged as outer join
        data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {result.hash}")
        row_count = data_service.pgi.fetch_one()["count"]
        assert row_count >= 2  # Should include all rows from both tables

    def test_complex_relationship_merge_with_comments(self):
        """Test relationship merge with CO (comments) dataset."""
        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, COMPLEX_RELATIONSHIP_DATA)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        result = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ae"],
            relationship_dataset=schemas["co"],
            domain="CO",
            relationship_columns=relationship_columns,
        )

        # Verify schema includes CO columns
        assert result.has_column("COREFID.CO")
        assert result.has_column("COREF.CO")

        # Verify filtering worked - should only include records that match relationship conditions
        data_service.pgi.execute_sql(
            f"""
            SELECT COUNT(*) as count FROM {result.hash}
            WHERE {result.get_column_hash('COREFID.CO')} IS NOT NULL
        """
        )
        co_count = data_service.pgi.fetch_one()["count"]
        assert co_count > 0

    def test_validation_missing_columns(self):
        """Test validation when required columns are missing."""
        data_service = PostgresQLDataService.instance()

        # Create test data with missing USUBJID in original table
        invalid_data = {
            "ec": {
                "STUDYID": ["STUDY001"],
                "DOMAIN": ["EC"],
                "ECSEQ": [1],
            },
            "relsub": {
                "STUDYID": ["STUDY001"],
                "USUBJID": ["SUBJ001"],
                "IDVAR": ["ECSEQ"],
                "IDVARVAL": ["1"],
            },
        }
        schemas = self._load_test_data(data_service, invalid_data)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        with pytest.raises(ValueError, match="Original schema missing required column"):
            SqlRelationshipMerge.perform_join(
                pgi=data_service.pgi,
                original=schemas["ec"],
                relationship_dataset=schemas["relsub"],
                domain="RELSUB",
                relationship_columns=relationship_columns,
            )

    def test_validation_missing_relationship_columns(self):
        """Test validation when relationship columns don't exist."""
        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, SIMPLE_RELATIONSHIP_DATA)

        relationship_columns = {"column_with_names": "NONEXISTENT", "column_with_values": "IDVARVAL"}

        with pytest.raises(ValueError, match=r"Right \(relationship\) schema missing column"):
            SqlRelationshipMerge.perform_join(
                pgi=data_service.pgi,
                original=schemas["ec"],
                relationship_dataset=schemas["relsub"],
                domain="RELSUB",
                relationship_columns=relationship_columns,
            )

    def test_rdomain_filtering(self):
        """Test that RDOMAIN filtering works correctly."""
        # Create data with different domains
        mixed_domain_data = {
            "ae": {
                "STUDYID": ["STUDY001", "STUDY001"],
                "DOMAIN": ["AE", "EC"],  # Mixed domains
                "USUBJID": ["SUBJ001", "SUBJ001"],
                "AESEQ": [1, 2],
            },
            "relsub": {
                "STUDYID": ["STUDY001"],
                "RDOMAIN": ["AE"],  # Only references AE domain
                "USUBJID": ["SUBJ001"],
                "IDVAR": ["AESEQ"],
                "IDVARVAL": ["1"],
                "POOLID": ["POOL1"],
            },
        }

        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, mixed_domain_data)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        result = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ae"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Should only include AE records, not EC records
        data_service.pgi.execute_sql(
            f"""
            SELECT {schemas['ae'].get_column_hash('DOMAIN')} as domain
            FROM {result.hash}
            WHERE {result.get_column_hash('POOLID.RELSUB')} IS NOT NULL
        """
        )
        domains = [row["domain"] for row in data_service.pgi.fetch_all()]
        assert all(domain == "AE" for domain in domains)

    def test_relationship_column_filtering(self):
        """Test that nested relationship column filtering works correctly."""
        # Create data where only specific values should match
        filtered_data = {
            "ec": {
                "STUDYID": ["STUDY001", "STUDY001", "STUDY001"],
                "DOMAIN": ["EC", "EC", "EC"],
                "USUBJID": ["SUBJ001", "SUBJ001", "SUBJ001"],
                "ECSEQ": [1, 2, 3],
                "ECSTDY": [1, 5, 10],
            },
            "relsub": {
                "STUDYID": ["STUDY001", "STUDY001"],
                "RDOMAIN": ["EC", "EC"],
                "USUBJID": ["SUBJ001", "SUBJ001"],
                "IDVAR": ["ECSEQ", "ECSEQ"],
                "IDVARVAL": ["1", "3"],  # Only matches ECSEQ 1 and 3, not 2
                "POOLID": ["POOL1", "POOL2"],
            },
        }

        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, filtered_data)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        result = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ec"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Check which ECSEQ values have corresponding RELSUB data
        data_service.pgi.execute_sql(
            f"""
            SELECT {schemas['ec'].get_column_hash('ECSEQ')} as ecseq
            FROM {result.hash}
            WHERE {result.get_column_hash('POOLID.RELSUB')} IS NOT NULL
            ORDER BY ecseq
        """
        )
        matched_ecseq = [row["ecseq"] for row in data_service.pgi.fetch_all()]

        # Should only match ECSEQ 1 and 3
        assert 1 in matched_ecseq
        assert 3 in matched_ecseq
        assert 2 not in matched_ecseq

    def test_table_caching(self):
        """Test that tables are cached and reused."""
        data_service = PostgresQLDataService.instance()
        schemas = self._load_test_data(data_service, SIMPLE_RELATIONSHIP_DATA)

        relationship_columns = {"column_with_names": "IDVAR", "column_with_values": "IDVARVAL"}

        # First merge
        result1 = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ec"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Second merge with same parameters
        result2 = SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=schemas["ec"],
            relationship_dataset=schemas["relsub"],
            domain="RELSUB",
            relationship_columns=relationship_columns,
        )

        # Should return the same cached table
        assert result1.name == result2.name
        assert result1.hash == result2.hash
