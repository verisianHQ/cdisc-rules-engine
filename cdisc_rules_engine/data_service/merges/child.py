from typing import Dict, List, Optional, Tuple

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.util import numeric_aware_key_sql, quote_literal_sql
from cdisc_rules_engine.exceptions.custom_exceptions import MatchDatasetNotFoundError
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.standards.sdtm_dataset_metadata import SdtmDatasetMetadata2
from cdisc_rules_engine.utilities.utils import (
    get_sided_match_keys,
    replace_pattern_in_list_of_strings,
)

LINK_IDVAR = "_link_idvar"
LINK_KEY = "_link_key"


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
        The same goes for any child naming no parent at all: one with no records, or a CO
        that omits the Permissible RDOMAIN/IDVAR/IDVARVAL columns. Any other child whose
        RDOMAIN names only datasets that aren't present skips the rule, like any missing
        match dataset.
        """
        # Find parent dataset(s)
        parent_metadatas = SqlChildMerge._find_parents(
            pgi=pgi,
            child=child,
            datasets=datasets,
            merge_spec=merge_spec,
        )

        if not parent_metadatas:
            rdomain_values = SqlChildMerge._get_ordered_rdomain_values(pgi, child)
            if SqlChildMerge._is_relational_child(child) or not rdomain_values:
                return child
            raise MatchDatasetNotFoundError(f"Match dataset {', '.join(rdomain_values)} is not present")

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
                    links=child,
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
        links: Optional[SqlTableSchema] = None,
    ) -> SqlTableSchema:
        """
        Join a relational (SUPP--/SQ--/CO/RELREC) child onto its parent.
        `links` is the child's own dataset, from which its IDVARs and IDVARVALs are read
        (by default `child`, which is wider and possibly longer
        once earlier parents have been joined onto it, but holds the same links).

        Mirrors the linking rules SqlSuppMerge applies - STUDYID/USUBJID/RDOMAIN plus a
        dynamic match of IDVARVAL against the parent column named by IDVAR - without
        requiring the caller to spell them out via match_key.

        IDVARVAL is matched through a key computed once per row on each side (see
        numeric_aware_key_sql) meaning we can hash the join with postgresql instead of testing every
        child row against every parent row of the same subject.
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

        links = links if links is not None else child
        link = {}
        idvar_rows = SqlChildMerge._find_linking_idvars(pgi, links, parent, parent_domain)
        if idvar_rows:
            idvarval_key = numeric_aware_key_sql(f"l.{child.get_column_hash('idvarval')}")
            link = {
                "extra_conditions": [
                    f"l.{child.get_column_hash('idvar')} = r.{LINK_IDVAR}",
                    f"{idvarval_key} = r.{LINK_KEY}",
                ],
                "right_source": SqlChildMerge._parent_keyed_by_idvar(
                    pgi, parent, idvar_rows, links, list(zip(pivot_left, pivot_right))
                ),
            }

        return SqlJoinMerge.perform_join(
            pgi=pgi,
            left=child,
            right=parent,
            pivot_left=pivot_left,
            pivot_right=pivot_right,
            type="LEFT",
            **link,
        )

    @staticmethod
    def _parent_keyed_by_idvar(
        pgi: PostgresQLInterface,
        parent: SqlTableSchema,
        idvar_rows: Dict[str, int],
        links: SqlTableSchema,
        pivots: List[Tuple[str, str]],
    ) -> str:
        """
        The parent's rows once per IDVAR, each carrying that IDVAR and the key of the column it names.
        For the IDVARs that few child rows use
        (fewer than half as many as the parent has rows, e.g. FAGRPID in RELREC),
        only the parent rows that a child row names through that IDVAR are kept, as no other row can be joined
        (otherwise all of FA is copied, and sorted, for the one RELREC record using FAGRPID).
        These rows are stored in an analysed table, as Postgres can't estimate how many are kept,
        and would then pick a join testing every child row against every parent row of the same subject.
        """

        # (child column, parent column) pairs that the join compares as they are, so they can be filtered on too
        filter_pivots = [
            (links.get_column_hash(child_column), parent.get_column_hash(parent_column))
            for child_column, parent_column in pivots
            if links.has_column(child_column)
            and links.get_column(child_column).type == parent.get_column(parent_column).type
        ]
        linked_key = numeric_aware_key_sql(f"c.{links.get_column_hash('idvarval')}")

        def keyed_rows(idvar: str, linked_only: bool) -> str:
            idvar_sql = quote_literal_sql(idvar)
            key = numeric_aware_key_sql(f"p.{parent.get_column_hash(idvar)}")
            rows = f"SELECT p.*, {idvar_sql}::text AS {LINK_IDVAR}, {key} AS {LINK_KEY} FROM {parent.hash} p"
            if linked_only:
                parent_values = [f"p.{parent_hash}" for _, parent_hash in filter_pivots] + [key]
                child_values = [f"c.{child_hash}" for child_hash, _ in filter_pivots] + [linked_key]
                rows += (
                    f" WHERE ({', '.join(parent_values)}) IN (SELECT {', '.join(child_values)} FROM {links.hash} c"
                    f" WHERE c.{links.get_column_hash('idvar')} = {idvar_sql})"
                )
            return rows

        pgi.execute_sql(f"SELECT COALESCE(MAX(id), 0) AS n FROM {parent.hash}")
        parent_rows = pgi.fetch_all()[0]["n"]
        sparse_idvars = [idvar for idvar, rows in idvar_rows.items() if rows * 2 < parent_rows]
        sources = [keyed_rows(idvar, linked_only=False) for idvar in idvar_rows if idvar not in sparse_idvars]
        if sparse_idvars:
            linked = SqlTableSchema.derived(f"{parent.name}_LINKED_FROM_{links.name}", pgi).hash
            pgi.execute_sql(f"SELECT to_regclass({quote_literal_sql(linked)}) IS NOT NULL AS stored")
            if not pgi.fetch_all()[0]["stored"]:
                linked_rows = " UNION ALL ".join(keyed_rows(idvar, linked_only=True) for idvar in sparse_idvars)
                pgi.execute_sql(f"CREATE UNLOGGED TABLE {linked} AS {linked_rows}")
                pgi.execute_sql(f"ANALYZE {linked}")
            sources.append(f"SELECT * FROM {linked}")
        return f"({' UNION ALL '.join(sources)})"

    @staticmethod
    def _find_linking_idvars(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        parent: SqlTableSchema,
        parent_domain: str,
    ) -> Dict[str, int]:
        """
        The child's IDVAR values that name a real column on the parent, i.e. the parent
        columns its IDVARVALs are matched against (e.g. IDVAR='AESEQ' -> parent.AESEQ),
        with the number of child rows using each.

        Restricted to rows relating to parent_domain: datasets like RELREC hold rows for
        many different RDOMAIN values in one table (e.g. AESEQ for AE-side rows, LBSEQ for
        LB-side rows), so IDVAR values from unrelated domains must not be considered when
        linking to this particular parent.

        Empty when there are no matching non-null IDVAR values (e.g. for SUPPDM-style
        parents where USUBJID alone identifies the single parent record), or when none of the
        IDVAR values found name a real column on the parent.
        """
        idvar_hash = child.get_column_hash("idvar")

        where_clauses = [f"{idvar_hash} IS NOT NULL"]
        if child.has_column("rdomain"):
            rdomain_hash = child.get_column_hash("rdomain")
            escaped_domain = parent_domain.replace("'", "''")
            where_clauses.append(f"{rdomain_hash} = '{escaped_domain}'")

        pgi.execute_sql(
            f"SELECT {idvar_hash} AS col, COUNT(*) AS n FROM {child.hash} "
            f"WHERE {' AND '.join(where_clauses)} GROUP BY 1 ORDER BY 1"
        )
        idvar_rows = {row["col"]: row["n"] for row in pgi.fetch_all() if row.get("col")}

        linking = {}
        for value, rows in idvar_rows.items():
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
            linking[value] = rows
        return linking

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
