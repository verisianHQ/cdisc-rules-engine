"""
Child merge logic for SQL implementation.

Child merges perform LEFT JOIN operations where the child dataset is preserved
and enriched with data from its parent dataset.
"""

from typing import List, Optional

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.utilities.utils import (
    get_sided_match_keys,
    replace_pattern_in_list_of_strings,
)


class SqlChildMerge:
    """Handles child-to-parent merge operations using LEFT JOIN."""

    @staticmethod
    def perform_merge(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        child_domain: str,
        datasets: List[SDTMDatasetMetadata],
        merge_spec: dict,
    ) -> SqlTableSchema:
        """
        Perform child merge: Find parent dataset and LEFT JOIN child with parent.

        Child dataset is on the left, parent on the right.
        Uses SqlJoinMerge with type="LEFT".

        Args:
            pgi: PostgreSQL interface
            child: Child dataset schema
            child_domain: Domain of the child dataset
            datasets: List of available datasets
            merge_spec: Merge specification from rule

        Returns:
            Resulting merged table schema

        Raises:
            ValueError: If no parent dataset is found
        """
        # Find parent dataset
        parent_metadata = SqlChildMerge._find_parent(
            pgi=pgi,
            child=child,
            datasets=datasets,
            merge_spec=merge_spec,
        )

        if not parent_metadata:
            raise ValueError(f"Could not find parent dataset for child merge: {child.name}")

        # Extract and process match keys
        match_keys = merge_spec.get("match_key", [])
        child_keys = get_sided_match_keys(match_keys, "left")
        parent_keys = get_sided_match_keys(match_keys, "right")

        # Replace "--" pattern with actual domain names
        child_keys = replace_pattern_in_list_of_strings(child_keys, "--", child_domain)
        parent_keys = replace_pattern_in_list_of_strings(parent_keys, "--", parent_metadata.domain)

        # Perform LEFT JOIN
        parent = pgi.schema.get_table(parent_metadata.name)
        result_schema = SqlJoinMerge.perform_join(
            pgi=pgi,
            left=child,
            right=parent,
            pivot_left=child_keys,
            pivot_right=parent_keys,
            type="LEFT",
        )

        return result_schema

    @staticmethod
    def _find_parent(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SDTMDatasetMetadata],
        merge_spec: dict,
    ) -> Optional[SDTMDatasetMetadata]:
        """
        Find parent dataset for a child - mirrors Python logic.

        Strategies (in priority order):
        1. Child has RDOMAIN column → find dataset matching RDOMAIN values
        2. Match key-based fallback → find dataset with required columns

        Args:
            pgi: PostgreSQL interface
            child: Child dataset schema
            datasets: List of available datasets
            merge_spec: Merge specification from rule

        Returns:
            Parent dataset metadata, or None if not found
        """
        # Strategy 1: RDOMAIN column in child data (works for CO, RELREC, SUPPLB, etc.)
        if child.has_column("rdomain"):
            rdomain_hash = child.get_column_hash("rdomain")
            pgi.execute_sql(
                f"SELECT DISTINCT {rdomain_hash} as rdomain " f"FROM {child.hash} WHERE {rdomain_hash} IS NOT NULL"
            )
            rdomain_values = [row["rdomain"] for row in pgi.fetch_all() if row.get("rdomain")]

            if rdomain_values:
                # Return first matching parent
                return next(
                    (ds for ds in datasets if ds.domain in rdomain_values),
                    None,
                )

        # Strategy 2: Match key-based fallback (if no RDOMAIN column)
        # Note: Most real data has RDOMAIN, so this is rarely needed
        match_keys = merge_spec.get("match_key", [])
        if match_keys:
            parent_keys = get_sided_match_keys(match_keys, "right")

            # Find first dataset with all required keys
            for ds in datasets:
                if ds.name == child.name:
                    continue  # Skip child itself
                table = pgi.schema.get_table(ds.name)
                if table and all(table.has_column(key.lower()) for key in parent_keys):
                    return ds

        return None
