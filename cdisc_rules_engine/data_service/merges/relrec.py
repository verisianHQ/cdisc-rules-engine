from typing import List, Dict, Optional, Set, Tuple

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.util import numeric_aware_key_sql, quote_literal_sql
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

# a pair links specific records when its left IDVARVAL is set, otherwise it links the IDVAR columns themselves
BY_VALUE_SQL = "idvarval_left <> ''"
# a pair names a subject unless its USUBJID is blank
BY_SUBJECT_SQL = r"usubjid ~ '\S'"
# how the merge finds the linked records (see SqlRelrecMerge._merge_queries)
LINK_PLANNER_SETTINGS = {"join_collapse_limit": "1", "enable_nestloop": "off", "enable_mergejoin": "off"}


class SqlRelrecMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relrec: SqlTableSchema,
        domain: str,
        wildcard: str = "__",
        model_wildcard_variables: Optional[Set[str]] = None,
    ) -> SqlTableSchema:
        """
        Perform a RELREC merge operation on the datasets.

        This merge creates relationships between records based on the RELREC dataset:
        1. Pair RELREC records for the target domain with their related records
        2. Find all related domains through those pairs
        3. For each kind of pair, join the original dataset with the related dataset through the pairs
        4. Apply wildcard column renaming (e.g., ECSTDY -> RELREC.__STDY)
        5. Union all results
        """
        # Validate required columns exist
        SqlRelrecMerge._validate_merge(original, relrec, domain)

        name = f"{original.name}_RELREC_{wildcard}"

        # Check if the table already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        relationships = SqlRelrecMerge._relationship_kinds(pgi, relrec, domain)

        if not relationships:
            return original

        # Build the merged schema with renamed columns
        schema = SqlRelrecMerge._build_merged_schema(
            pgi, name, original, relationships, wildcard, model_wildcard_variables
        )
        pgi.create_table(schema)

        # Process each relationship and union results
        SqlRelrecMerge._process_relationship_records(
            pgi,
            schema,
            original,
            relationships,
            SqlRelrecMerge._relationship_pairs_sql(relrec, domain),
            wildcard,
            model_wildcard_variables,
        )

        return schema

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        relrec: SqlTableSchema,
        domain: str,
    ):
        """
        Validate that the required columns exist for the RELREC merge.
        """
        required_original_columns = ["STUDYID", "USUBJID"]
        required_relrec_columns = ["STUDYID", "USUBJID", "RELID", "RDOMAIN"]

        for col in required_original_columns:
            if not original.has_column(col):
                raise ColumnNotFoundError(col, original.dataset_name)

        for col in required_relrec_columns:
            if not relrec.has_column(col):
                raise ColumnNotFoundError(col, relrec.dataset_name)

    @staticmethod
    def _relationship_kinds(
        pgi: PostgresQLInterface,
        relrec: SqlTableSchema,
        domain: str,
    ) -> List[Dict]:
        """
        The distinct kinds of relationship pair for the target domain: right domain, IDVARs,
        and whether the pair links by value and by subject.
        Each kind becomes one join.
        """
        pgi.execute_sql(
            f"""SELECT DISTINCT rdomain_right, idvar_left, idvar_right,
                    {BY_VALUE_SQL} AS by_value, {BY_SUBJECT_SQL} AS by_subject
                FROM ({SqlRelrecMerge._relationship_pairs_sql(relrec, domain)}) pairs
                ORDER BY 1, 2, 3, 4, 5"""
        )
        return pgi.fetch_all()

    @staticmethod
    def _relationship_pairs_sql(relrec: SqlTableSchema, domain: str) -> str:
        """
        RELREC records for the target domain paired with the records they relate to.

        This is the SQL equivalent of the Python filter_relrec_for_domain function.
        """
        return f"""
            SELECT
                left_rel.{relrec.get_column_hash("STUDYID")} as studyid,
                left_rel.{relrec.get_column_hash("USUBJID")} as usubjid,
                left_rel.{relrec.get_column_hash("RELID")} as relid,
                left_rel.{relrec.get_column_hash("RDOMAIN")} as rdomain_left,
                COALESCE(left_rel.{relrec.get_column_hash("IDVAR")}, '') as idvar_left,
                COALESCE(left_rel.{relrec.get_column_hash("IDVARVAL")}, '') as idvarval_left,
                right_rel.{relrec.get_column_hash("RDOMAIN")} as rdomain_right,
                COALESCE(right_rel.{relrec.get_column_hash("IDVAR")}, '') as idvar_right,
                COALESCE(right_rel.{relrec.get_column_hash("IDVARVAL")}, '') as idvarval_right
            FROM {relrec.hash} left_rel
            INNER JOIN {relrec.hash} right_rel
                ON left_rel.{relrec.get_column_hash("STUDYID")} = right_rel.{relrec.get_column_hash("STUDYID")}
                AND left_rel.{relrec.get_column_hash("USUBJID")} = right_rel.{relrec.get_column_hash("USUBJID")}
                AND left_rel.{relrec.get_column_hash("RELID")} = right_rel.{relrec.get_column_hash("RELID")}
            WHERE left_rel.{relrec.get_column_hash("RDOMAIN")} = {quote_literal_sql(domain)}
                AND right_rel.{relrec.get_column_hash("RDOMAIN")} != {quote_literal_sql(domain)}
        """

    # used only when the model metadata isn't available
    FALLBACK_WILDCARD_SUFFIXES = frozenset({"STDY", "ENDY", "DY", "TM", "DTC", "SEQ"})

    @staticmethod
    def _apply_wildcard_renaming(
        pgi: PostgresQLInterface,
        right_table: SqlTableSchema,
        domain: str,
        wildcard: str,
        model_wildcard_variables: Optional[Set[str]] = None,
    ) -> Dict[str, str]:
        """
        Apply wildcard renaming to columns from related domains.

        This is the SQL equivalent of the add_variable_wildcards function.
        A domain-prefixed column like AETERM is renamed to RELREC.<wildcard>TERM
        when --TERM is a variable defined by the model.
        Columns that are not model wildcard variables keep their own name under the
        RELREC prefix.
        """
        renamed_columns = {}

        for col_name, column in right_table.get_columns():
            if col_name == "id":
                continue

            column_upper = col_name.upper()
            domain_upper = domain.upper()

            if column_upper.startswith(domain_upper):
                suffix = column_upper[len(domain_upper) :]
                if SqlRelrecMerge._is_model_wildcard_variable(suffix, model_wildcard_variables):
                    renamed_columns[col_name] = f"RELREC.{wildcard}{suffix}"
                else:
                    # Keep original name but with RELREC prefix
                    renamed_columns[col_name] = f"RELREC.{col_name}"
            else:
                # Keep original name but with RELREC prefix
                renamed_columns[col_name] = f"RELREC.{col_name}"

        return renamed_columns

    @staticmethod
    def _is_model_wildcard_variable(
        suffix: str,
        model_wildcard_variables: Optional[Set[str]],
    ) -> bool:
        """
        Decide whether a domain-stripped suffix names a "--" variable in the model.
        """
        if not suffix:
            return False
        if model_wildcard_variables:
            return f"--{suffix}" in model_wildcard_variables
        return suffix in SqlRelrecMerge.FALLBACK_WILDCARD_SUFFIXES or suffix.startswith("_")

    @staticmethod
    def _build_merged_schema(
        pgi: PostgresQLInterface,
        name: str,
        original: SqlTableSchema,
        relationships: List[Dict],
        wildcard: str,
        model_wildcard_variables: Optional[Set[str]] = None,
    ) -> SqlTableSchema:
        """
        Build the output schema including original columns and renamed relationship columns.
        """
        schema = SqlTableSchema.derived(name, pgi)

        # Add all original columns including id
        for _, column in original.get_columns():
            schema.add_column(column)

        # For each unique right domain, determine what columns to add
        right_domains = set(rel["rdomain_right"] for rel in relationships)

        for right_domain in right_domains:
            # Try to find the table for this domain in the schema
            right_table = None
            for table_name, table_schema in pgi.schema.get_tables():
                if table_name.lower().startswith(right_domain.lower()):
                    right_table = table_schema
                    break

            if right_table:
                # Get column mappings for this domain
                column_mappings = SqlRelrecMerge._apply_wildcard_renaming(
                    pgi, right_table, right_domain, wildcard, model_wildcard_variables
                )
                SqlRelrecMerge._add_mapped_columns(schema, right_table, column_mappings)

        return schema

    @staticmethod
    def _add_mapped_columns(
        schema: SqlTableSchema,
        right_table: SqlTableSchema,
        column_mappings: Dict[str, str],
    ):
        """Add mapped columns to the schema with proper types."""
        for original_col, renamed_col in column_mappings.items():
            if not schema.has_column(renamed_col):
                # Get original column type
                original_column = None
                for col_name, col in right_table.get_columns():
                    if col_name == original_col:
                        original_column = col
                        break

                if original_column:
                    new_col_schema = SqlColumnSchema.generated(column=renamed_col, type=original_column.type)
                    schema.add_column(new_col_schema)

    @staticmethod
    def _process_relationship_records(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationships: List[Dict],
        pairs_sql: str,
        wildcard: str,
        model_wildcard_variables: Optional[Set[str]] = None,
    ):
        """
        Process each relationship record and populate the result table.
        """
        if not relationships:
            SqlRelrecMerge._copy_original_data_only(pgi, schema, original)
            return

        # Group relationships by right domain and process
        domain_relationships = SqlRelrecMerge._group_relationships_by_domain(relationships)
        pairs_table, links_table = f"pairs_{schema.hash}", f"links_{schema.hash}"
        link_parts, inserts = SqlRelrecMerge._build_domain_queries(
            pgi,
            schema,
            original,
            domain_relationships,
            pairs_table,
            links_table,
            wildcard,
            model_wildcard_variables,
        )

        # Execute all queries (in one transaction, which the temporary tables last for)
        if inserts:
            pgi.execute_many(SqlRelrecMerge._merge_queries(pairs_table, pairs_sql, links_table, link_parts, inserts))

    @staticmethod
    def _merge_queries(
        pairs_table: str,
        pairs_sql: str,
        links_table: str,
        link_parts: List[str],
        inserts: List[str],
    ) -> List[str]:
        """
        The merge's statements. They run in one transaction, and the temporary tables are dropped when it commits.

        1. The pairs go into an analysed table, made distinct and with their link keys computed once.
           Read straight from RELREC, Postgres estimates them at about one row
           and picks nested loops that go through all of the subject's records for every pair.
        2. The ids of the linked records go into a second analysed table,
           made distinct (several pairs can link the same two records).
           These joins keep their written order (records with their pairs first, then the related records)
           and use hash joins only, as the planner cannot estimate them and tends to pick far slower plans.
        3. The planner settings go back to their defaults, and the linked records are read by id
           into the merged table, with Postgres free to plan this step.
        """
        return [
            f"""
            CREATE TEMPORARY TABLE {pairs_table} ON COMMIT DROP AS
            SELECT DISTINCT
                UPPER(studyid::text) AS studyid,
                UPPER(usubjid::text) AS usubjid,
                rdomain_right,
                idvar_left,
                idvar_right,
                {BY_VALUE_SQL} AS by_value,
                {BY_SUBJECT_SQL} AS by_subject,
                {numeric_aware_key_sql('idvarval_left')} AS key_left,
                {numeric_aware_key_sql('idvarval_right')} AS key_right
            FROM ({pairs_sql}) pairs
            """,
            f"ANALYZE {pairs_table}",
            *(f"SET LOCAL {name} = {value}" for name, value in LINK_PLANNER_SETTINGS.items()),
            f"""
            CREATE TEMPORARY TABLE {links_table} ON COMMIT DROP AS
            SELECT DISTINCT right_domain, original_id, right_id FROM (
                {' UNION ALL '.join(link_parts)}
            ) AS links
            """,
            f"ANALYZE {links_table}",
            *(f"SET LOCAL {name} TO DEFAULT" for name in LINK_PLANNER_SETTINGS),
            *inserts,
        ]

    @staticmethod
    def _copy_original_data_only(pgi: PostgresQLInterface, schema: SqlTableSchema, original: SqlTableSchema):
        """Copy original data when no relationships exist."""
        source_columns = [col.hash for col_name, col in original.get_columns() if col_name != "id"]
        target_columns = [col.hash for col_name, col in schema.get_columns() if col_name != "id"]
        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(source_columns)}
            FROM {original.hash}
            ORDER BY id
        """
        pgi.execute_sql(query)

    @staticmethod
    def _group_relationships_by_domain(relationships: List[Dict]) -> Dict[str, List[Dict]]:
        """Group relationships by right domain."""
        domain_relationships = {}
        for rel in relationships:
            right_domain = rel["rdomain_right"]
            if right_domain not in domain_relationships:
                domain_relationships[right_domain] = []
            domain_relationships[right_domain].append(rel)
        return domain_relationships

    @staticmethod
    def _build_domain_queries(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        domain_relationships: Dict[str, List[Dict]],
        pairs_table: str,
        links_table: str,
        wildcard: str,
        model_wildcard_variables: Optional[Set[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Build the queries finding the linked records, and the inserts reading them, for all related domains."""
        link_parts, inserts = [], []
        for right_domain, domain_rels in domain_relationships.items():
            right_table = pgi.schema.get_table(right_domain.lower())
            if right_table:
                queries = SqlRelrecMerge._build_single_domain_query(
                    pgi,
                    schema,
                    original,
                    right_table,
                    right_domain,
                    domain_rels,
                    pairs_table,
                    links_table,
                    wildcard,
                    model_wildcard_variables,
                )
                if queries:
                    domain_link_parts, insert = queries
                    link_parts.extend(domain_link_parts)
                    inserts.append(insert)
        return link_parts, inserts

    @staticmethod
    def _build_single_domain_query(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        right_table: SqlTableSchema,
        right_domain: str,
        domain_rels: List[Dict],
        pairs_table: str,
        links_table: str,
        wildcard: str,
        model_wildcard_variables: Optional[Set[str]] = None,
    ) -> Optional[Tuple[List[str], str]]:
        """
        Build the queries finding the records linked in one related domain, and the insert reading them
        (None when no pair links records of this domain).
        """
        column_mappings = SqlRelrecMerge._apply_wildcard_renaming(
            pgi, right_table, right_domain, wildcard, model_wildcard_variables
        )

        # Build select clauses and target columns
        original_selects, right_selects, target_columns = SqlRelrecMerge._build_select_clauses(
            original, right_table, schema, column_mappings
        )

        # Build union parts for all relationships
        union_parts = SqlRelrecMerge._build_union_parts(original, right_table, right_domain, domain_rels, pairs_table)

        if not union_parts:
            return None

        # the parts only find the linked records' ids, which are made distinct (see _merge_queries),
        # and the records are read once at the end
        original_id, right_id = original.get_column_hash("id"), right_table.get_column_hash("id")
        return (
            union_parts,
            f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(original_selects + right_selects)}
            FROM {links_table} l
            INNER JOIN {original.hash} o ON o.{original_id} = l.original_id
            INNER JOIN {right_table.hash} r ON r.{right_id} = l.right_id
            WHERE l.right_domain = {quote_literal_sql(right_domain)}
            ORDER BY l.original_id, l.right_id
        """,
        )

    @staticmethod
    def _build_select_clauses(original, right_table, schema, column_mappings):
        """Build select clauses and target columns (without id, which the merged table generates)."""
        original_selects = []
        right_selects = []
        target_columns = []

        # Original table columns
        for col_name, col in original.get_columns():
            if col_name != "id":
                original_selects.append(f"o.{col.hash}")
                target_columns.append(col.hash)

        # Right table columns (renamed)
        for original_col, renamed_col in column_mappings.items():
            if schema.has_column(renamed_col):
                right_col_hash = right_table.get_column_hash(original_col)
                renamed_col_hash = schema.get_column_hash(renamed_col)
                right_selects.append(f"r.{right_col_hash} AS {renamed_col_hash}")
                target_columns.append(renamed_col_hash)

        return original_selects, right_selects, target_columns

    @staticmethod
    def _build_union_parts(original, right_table, right_domain, domain_rels, pairs_table):
        """
        One query per kind of pair, giving the ids of the original and related records that its pairs link.
        """
        union_parts = []
        for rel in domain_rels:
            idvar_left, idvar_right = rel["idvar_left"], rel["idvar_right"]
            # pairs link records through the columns their IDVARs name, so
            # pairs whose IDVAR is blank or names a variable that isn't in the dataset
            # link nothing, as in the child merge (without those columns' condition,
            # every record would be linked to all of the subject's related records)
            if not (original.has_column(idvar_left) and right_table.has_column(idvar_right)):
                continue

            pair_filters = [
                f"rdomain_right = {quote_literal_sql(right_domain)}",
                f"idvar_left = {quote_literal_sql(idvar_left)}",
                f"idvar_right = {quote_literal_sql(idvar_right)}",
                f"by_value IS {'TRUE' if rel['by_value'] else 'NOT TRUE'}",
                f"by_subject IS {'TRUE' if rel['by_subject'] else 'NOT TRUE'}",
            ]
            pair_conditions = [f"UPPER(o.{original.get_column_hash('STUDYID')}::text) = p.studyid"]
            if rel["by_subject"]:
                pair_conditions.append(f"UPPER(o.{original.get_column_hash('USUBJID')}::text) = p.usubjid")
            join_conditions = [
                f"o.{original.get_column_hash('STUDYID')} = r.{right_table.get_column_hash('STUDYID')}",
                f"o.{original.get_column_hash('USUBJID')} = r.{right_table.get_column_hash('USUBJID')}",
            ]

            left_value = f"o.{original.get_column_hash(idvar_left)}"
            left_key = numeric_aware_key_sql(left_value)
            right_key = numeric_aware_key_sql(f"r.{right_table.get_column_hash(idvar_right)}")
            if rel["by_value"]:
                pair_conditions.append(f"{left_key} = p.key_left")
                join_conditions.append(f"{right_key} = p.key_right")
                # a blank related IDVARVAL names no record, so it isn't linked to the records with a blank value
                pair_filters.append("TRIM(key_right) <> ''")
            else:
                join_conditions.append(f"{left_key} = {right_key}")
                # a record with a blank --GRPID/--LNKID is in no group, so it isn't linked to the other blank ones
                # (linking them related every such record to every other one of the subject)
                pair_conditions.append(f"TRIM(CAST({left_value} AS TEXT)) <> ''")

            original_id, right_id = original.get_column_hash("id"), right_table.get_column_hash("id")
            union_part = f"""
                SELECT
                    {quote_literal_sql(right_domain)}::text AS right_domain,
                    o.{original_id} AS original_id,
                    r.{right_id} AS right_id
                FROM {original.hash} o
                INNER JOIN (SELECT * FROM {pairs_table} WHERE {' AND '.join(pair_filters)}) p
                    ON {' AND '.join(pair_conditions)}
                INNER JOIN {right_table.hash} r ON {' AND '.join(join_conditions)}
            """
            union_parts.append(union_part)

        return union_parts
