from typing import List, Optional

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.standards.sdtm_dataset_metadata import SdtmDatasetMetadata2
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
        datasets: List[SdtmDatasetMetadata2],
        merge_spec: dict,
    ) -> SqlTableSchema:
        """
        Perform child merge: Find parent dataset(s) and LEFT JOIN child with parent(s).

        Child dataset is on the left, parent(s) on the right.
        Uses SqlJoinMerge with type="LEFT".

        For datasets like RELREC with multiple RDOMAIN values, this will merge
        with ALL matching parent datasets sequentially.

        Relational datasets (SUPP--, SQ--, CO, etc.) carry IDVAR/IDVARVAL columns that
        identify which parent record a row relates to. For those, the linking columns
        (STUDYID/USUBJID/RDOMAIN plus the dynamic IDVAR/IDVARVAL match) are derived
        automatically instead of requiring them to be spelled out in match_key.

        RDOMAIN/IDVAR/IDVARVAL are themselves optional for relational children like CO,
        which may contain standalone records (e.g. a general comment not tied to any
        parent record) that leave all three null. A relational child with no linkable
        RDOMAIN values at all is therefore not an error - it's simply returned unchanged.
        """
        # Find parent dataset(s)
        parent_metadatas = SqlChildMerge._find_parents(
            pgi=pgi,
            child=child,
            datasets=datasets,
            merge_spec=merge_spec,
        )

        if not parent_metadatas:
            if SqlChildMerge._is_relational_child(child):
                return child
            raise ValueError(f"Could not find parent dataset for child merge: {child.name}")

        # Perform sequential merges with each parent (already in correct order from _find_parents)
        result_schema = child
        for parent_metadata in parent_metadatas:
            parent = pgi.schema.get_table(parent_metadata.name)

            if SqlChildMerge._is_relational_child(result_schema):
                result_schema = SqlChildMerge._perform_relational_join(
                    pgi=pgi,
                    child=result_schema,
                    parent=parent,
                    parent_domain=parent_metadata.domain,
                )
                continue

            # Extract and process match keys
            match_keys = merge_spec.get("match_key", [])
            child_keys = get_sided_match_keys(match_keys, "left")
            parent_keys = get_sided_match_keys(match_keys, "right")

            # Replace "--" pattern with actual domain names
            child_keys = replace_pattern_in_list_of_strings(child_keys, "--", child_domain)
            parent_keys = replace_pattern_in_list_of_strings(parent_keys, "--", parent_metadata.domain)

            # Perform LEFT JOIN
            result_schema = SqlJoinMerge.perform_join(
                pgi=pgi,
                left=result_schema,
                right=parent,
                pivot_left=child_keys,
                pivot_right=parent_keys,
                type="LEFT",
            )

        return result_schema

    @staticmethod
    def _is_relational_child(child: SqlTableSchema) -> bool:
        """Detect SUPP--/SQ--/CO-style datasets that key off IDVAR/IDVARVAL."""
        return child.has_column("idvar") and child.has_column("idvarval")

    @staticmethod
    def _perform_relational_join(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        parent: SqlTableSchema,
        parent_domain: str,
    ) -> SqlTableSchema:
        """
        Join a relational (SUPP--/SQ--/CO/RELREC) child onto its parent.

        Mirrors the linking rules SqlSuppMerge applies - STUDYID/USUBJID/RDOMAIN plus a
        dynamic match of IDVARVAL against the parent column named by IDVAR - without
        requiring the caller to spell them out via match_key.
        """
        pivot_left: List[str] = []
        pivot_right: List[str] = []
        for column in ("studyid", "usubjid"):
            if child.has_column(column) and parent.has_column(column):
                pivot_left.append(column)
                pivot_right.append(column)

        if child.has_column("rdomain") and parent.has_column("domain"):
            pivot_left.append("rdomain")
            pivot_right.append("domain")

        idvar_condition = SqlChildMerge._find_idvar_linking_condition(pgi, child, parent, parent_domain)
        extra_conditions = [idvar_condition] if idvar_condition else None

        return SqlJoinMerge.perform_join(
            pgi=pgi,
            left=child,
            right=parent,
            pivot_left=pivot_left,
            pivot_right=pivot_right,
            type="LEFT",
            extra_conditions=extra_conditions,
        )

    @staticmethod
    def _find_idvar_linking_condition(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        parent: SqlTableSchema,
        parent_domain: str,
    ) -> Optional[str]:
        """
        Build a join condition matching each child row's IDVARVAL against the parent
        column named by that row's IDVAR (e.g. IDVAR='AESEQ' -> parent.AESEQ).

        Restricted to rows relating to parent_domain: datasets like RELREC hold rows for
        many different RDOMAIN values in one table (e.g. AESEQ for AE-side rows, LBSEQ for
        LB-side rows), so IDVAR values from unrelated domains must not be considered when
        linking to this particular parent.

        Returns None when there are no matching non-null IDVAR values (e.g. for SUPPDM-style
        parents where USUBJID alone identifies the single parent record), or when none of the
        IDVAR values found name a real column on the parent.
        """
        idvar_hash = child.get_column_hash("idvar")
        idvarval_hash = child.get_column_hash("idvarval")

        where_clauses = [f"{idvar_hash} IS NOT NULL"]
        if child.has_column("rdomain"):
            rdomain_hash = child.get_column_hash("rdomain")
            escaped_domain = parent_domain.replace("'", "''")
            where_clauses.append(f"{rdomain_hash} = '{escaped_domain}'")

        pgi.execute_sql(f"SELECT DISTINCT {idvar_hash} AS col FROM {child.hash} WHERE {' AND '.join(where_clauses)}")
        idvar_values = [row["col"] for row in pgi.fetch_all() if row.get("col")]

        if not idvar_values:
            return None

        cases = []
        for value in idvar_values:
            if not parent.has_column(value):
                # An IDVAR value that doesn't name a real column in the parent is invalid
                # data, not a reason to abort the merge - rows referencing it simply don't
                # link to anything (NULL), the same as any other non-matching join key.
                # Conformance rules validating IDVAR itself (e.g. CORE-000953) depend on
                # being able to run over exactly this kind of malformed data.
                logger.debug(
                    f"Child merge: parent dataset '{parent.name}' has no column '{value}' "
                    f"referenced by IDVAR in '{child.name}'; treating as unmatched."
                )
                continue
            parent_col_hash = parent.get_column_hash(value)
            escaped_value = value.replace("'", "''")
            cases.append(f"WHEN '{escaped_value}' THEN r.{parent_col_hash}::text")

        if not cases:
            return None

        case_expression = f"CASE l.{idvar_hash} {' '.join(cases)} END"
        return f"{case_expression} = l.{idvarval_hash}::text"

    @staticmethod
    def _get_ordered_rdomain_values(pgi: PostgresQLInterface, child: SqlTableSchema) -> List[str]:
        """Get unique RDOMAIN values from a table, preserving first-appearance order."""
        if not child.has_column("rdomain"):
            return []

        rdomain_hash = child.get_column_hash("rdomain")
        pgi.execute_sql(
            f"SELECT {rdomain_hash} as rdomain "
            f"FROM {child.hash} "
            f"WHERE {rdomain_hash} IS NOT NULL "
            f"ORDER BY id"
        )

        rdomain_values = []
        seen = set()
        for row in pgi.fetch_all():
            rdomain = row.get("rdomain")
            if rdomain and rdomain not in seen:
                rdomain_values.append(rdomain)
                seen.add(rdomain)
        return rdomain_values

    @staticmethod
    def _find_parents_by_rdomain(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SdtmDatasetMetadata2],
    ) -> List[SdtmDatasetMetadata2]:
        """Find parent datasets using the RDOMAIN column."""
        rdomain_values = SqlChildMerge._get_ordered_rdomain_values(pgi, child)
        if not rdomain_values:
            return []

        rdomain_set = set(rdomain_values)
        matching_parents = []
        seen_domains = set()
        for dataset in datasets:
            if dataset.domain in rdomain_set and dataset.domain not in seen_domains:
                matching_parents.append(dataset)
                seen_domains.add(dataset.domain)
        return matching_parents

    @staticmethod
    def _find_parents_by_match_keys(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SdtmDatasetMetadata2],
        merge_spec: dict,
    ) -> List[SdtmDatasetMetadata2]:
        """Find parent datasets using match keys as a fallback."""
        match_keys = merge_spec.get("match_key", [])
        if not match_keys:
            return []

        parent_keys = get_sided_match_keys(match_keys, "right")
        matching_parents = []
        for ds in datasets:
            if ds.name == child.name:
                continue

            table = pgi.schema.get_table(ds.name)
            if table and all(table.has_column(key.lower()) for key in parent_keys):
                matching_parents.append(ds)
        return matching_parents

    @staticmethod
    def _find_parents(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SdtmDatasetMetadata2],
        merge_spec: dict,
    ) -> List[SdtmDatasetMetadata2]:
        """
        Find parent dataset(s) for a child using RDOMAIN or match keys.
        """
        # Strategy 1: RDOMAIN column in child data (works for CO, RELREC, etc.)
        parents = SqlChildMerge._find_parents_by_rdomain(pgi, child, datasets)
        if parents:
            return parents

        # Strategy 2: Match key-based fallback
        # removing this strategy for now, as it causes many faulty joins with falsely identified parents
        # return SqlChildMerge._find_parents_by_match_keys(pgi, child, datasets, merge_spec)
        return []
