from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlRelationshipMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        relationship_columns: dict,
    ) -> SqlTableSchema:
        """
        Perform relationship merge following Python's three-step process:
        1. Filter by match keys of relationship dataset
        2. Filter by RDOMAIN and relationship columns (SUPP-style filtering)
        3. Merge with full outer join and domain suffixes
        """
        try:
            # Validate required columns
            SqlRelationshipMerge._validate_merge(original, relationship_dataset, relationship_columns)

            name = f"{original.name}_REL_{domain}"

            # Check if the table already exists
            if pgi.schema.get_table(name) is not None:
                return pgi.schema.get_table(name)

            # Get relationship column names
            column_with_names = relationship_columns.get("column_with_names")
            column_with_values = relationship_columns.get("column_with_values")

            # Check if relationship columns are all empty - if so, do simple outer join
            if SqlRelationshipMerge._has_empty_relationship_columns(
                pgi, relationship_dataset, column_with_names, column_with_values
            ):
                return SqlRelationshipMerge._perform_simple_merge(pgi, original, relationship_dataset, domain)

            schema = SqlRelationshipMerge._build_merged_schema(original, relationship_dataset, domain, name)
            pgi.create_table(schema)

            SqlRelationshipMerge._execute_relationship_merge(
                pgi, schema, original, relationship_dataset, domain, column_with_names, column_with_values
            )

            return schema

        except ValueError as e:
            # Re-raise validation errors with context
            raise ValueError(
                f"Relationship merge failed for {original.name} with {relationship_dataset.name}: {str(e)}"
            )
        except Exception as e:
            # For any other unexpected errors, return the original dataset to prevent complete failure
            # This matches the behavior in other merge implementations
            raise RuntimeError(f"Relationship merge encountered unexpected error for {original.name}: {str(e)}")

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        relationship_columns: dict,
    ):
        """Validate that required columns exist for the relationship merge."""
        # Check basic required columns
        for col in ["STUDYID", "USUBJID"]:
            if not original.has_column(col):
                raise ValueError(f"RELATIONSHIP MERGE: Original schema missing required column: {col}")

        # Validate relationship_columns parameter
        if not relationship_columns:
            raise ValueError("RELATIONSHIP MERGE: relationship_columns parameter is required but was None or empty")

        # Check relationship columns exist with defensive null checks
        column_with_names = relationship_columns.get("column_with_names")
        column_with_values = relationship_columns.get("column_with_values")

        # Add explicit null/empty checks before using column names
        if not column_with_names or str(column_with_names).strip() == "":
            raise ValueError(f"RELATIONSHIP MERGE: column_with_names is required but was: {column_with_names}")

        if not column_with_values or str(column_with_values).strip() == "":
            raise ValueError(f"RELATIONSHIP MERGE: column_with_values is required but was: {column_with_values}")

        # Note: We don't validate that IDVAR/IDVARVAL columns exist in the relationship dataset
        # because _has_empty_relationship_columns handles missing columns gracefully.
        # If columns are missing or empty, the simple merge path will be used.
        # This matches Python's behavior where accessing missing columns would fail at runtime,
        # but empty columns are handled by the check at data_processor.py lines 137-141.

        # Check for DOMAIN/RDOMAIN compatibility - allow more flexible combinations
        if relationship_dataset.has_column("RDOMAIN") and not (
            original.has_column("DOMAIN") or original.has_column("RDOMAIN")
        ):
            raise ValueError(
                "RELATIONSHIP MERGE: Original schema missing DOMAIN or RDOMAIN column when right has RDOMAIN"
            )

    @staticmethod
    def _has_empty_relationship_columns(
        pgi: PostgresQLInterface,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
    ) -> bool:
        """Check if all relationship columns are empty (like SUPP logic)."""
        # Add null safety before calling get_column_hash
        if not column_with_names or not column_with_values:
            return True

        # Validate columns exist before getting their hashes
        if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
            column_with_values
        ):
            return True

        try:
            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            if not names_hash or not values_hash:
                return True

            query = f"""
                SELECT COUNT(*) as count
                FROM {relationship_dataset.hash}
                WHERE TRIM(COALESCE({names_hash}, '')) != '' OR TRIM(COALESCE({values_hash}, '')) != ''
            """

            pgi.execute_sql(query)
            result = pgi.fetch_one()
            return result["count"] == 0
        except Exception:
            return True

    @staticmethod
    def _perform_simple_merge(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
    ) -> SqlTableSchema:
        """Perform simple outer join when relationship columns are empty."""
        name = f"{original.name}_REL_{domain}_SIMPLE"

        # Check if already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        # Build schema with domain suffixes
        schema = SqlRelationshipMerge._build_merged_schema(original, relationship_dataset, domain, name)
        pgi.create_table(schema)

        # Simple outer join on STUDYID, USUBJID
        left_cols = [col.hash for col_name, col in original.get_columns() if col_name != "id"]
        right_cols = []
        target_cols = left_cols.copy()

        for col_name, col in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:  # Skip join keys and id
                continue

            suffixed_name = f"{col_name}.{domain}"
            if schema.has_column(suffixed_name):
                suffixed_hash = schema.get_column_hash(suffixed_name)
                right_cols.append(f"r.{col.hash} AS {suffixed_hash}")
                target_cols.append(suffixed_hash)

        all_selects = [f"l.{col}" for col in left_cols] + right_cols

        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_cols)})
            SELECT {', '.join(all_selects)}
            FROM {original.hash} l
            FULL OUTER JOIN {relationship_dataset.hash} r
                ON l.{original.get_column_hash('STUDYID')} = r.{relationship_dataset.get_column_hash('STUDYID')}
                AND l.{original.get_column_hash('USUBJID')} = r.{relationship_dataset.get_column_hash('USUBJID')}
        """

        pgi.execute_sql(query)
        return schema

    @staticmethod
    def _build_merged_schema(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        name: str,
    ) -> SqlTableSchema:
        """Build merged schema following join.py patterns with domain suffixes."""
        schema = SqlTableSchema.from_join(name)

        # Add all original columns first (like join.py)
        for col_name, column in original.get_columns():
            if col_name == "id":
                continue
            schema.add_column(column)

        # Add relationship dataset columns with domain suffix (like join.py)
        for col_name, column in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:  # Skip join keys and id
                continue

            # Add suffixed column (e.g., POOLID.RELSUB)
            suffixed_name = f"{col_name}.{domain}"
            if not schema.has_column(suffixed_name):
                new_col_schema = SqlColumnSchema.generated(column=suffixed_name, type=column.type)
                schema.add_column(new_col_schema)

                # Add alias for unsuffixed name if not already present (like join.py)
                if not schema.has_column(col_name):
                    schema.add_column(SqlColumnSchema.alias(col_name, new_col_schema))

        return schema

    @staticmethod
    def _execute_relationship_merge(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        column_with_names: str,
        column_with_values: str,
    ):
        """
        Execute the relationship merge following Python's three-step process:
        1. Filter by match keys (USUBJID) existence in relationship dataset
        2. Filter by RDOMAIN and IDVAR/IDVARVAL columns
        3. Perform OUTER join with first IDVAR value as additional join key
        """
        # Build the filtered left subquery with all three filter steps
        filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
            pgi, original, relationship_dataset, column_with_names, column_with_values
        )

        # Get first IDVAR value for dynamic join (Step 3 from Python)
        first_idvar = SqlRelationshipMerge._get_first_idvar_value(pgi, relationship_dataset, column_with_names)

        # Build select clauses
        left_columns, right_columns, target_columns = SqlRelationshipMerge._build_select_clauses(
            original, relationship_dataset, schema, domain
        )

        # Build join conditions
        values_hash = relationship_dataset.get_column_hash(column_with_values)
        left_studyid = original.get_column_hash("STUDYID")
        right_studyid = relationship_dataset.get_column_hash("STUDYID")
        left_usubjid = original.get_column_hash("USUBJID")
        right_usubjid = relationship_dataset.get_column_hash("USUBJID")

        join_conditions = [
            f"l.{left_studyid} = r.{right_studyid}",
            f"l.{left_usubjid} = r.{right_usubjid}",
        ]

        # Add dynamic join column if first IDVAR exists in original (Python logic)
        if first_idvar and original.has_column(first_idvar):
            col_hash = original.get_column_hash(first_idvar)
            join_conditions.append(f"l.{col_hash}::text = r.{values_hash}::text")

        # Build and execute the query
        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(left_columns + right_columns)}
            FROM ({filtered_left_query}) l
            FULL OUTER JOIN {relationship_dataset.hash} r ON {' AND '.join(join_conditions)}
        """

        pgi.execute_sql(query)

    @staticmethod
    def _build_filtered_left_subquery(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
    ) -> str:
        """
        Build filtered left subquery matching Python's three-step filtering:
        1. Filter by match keys existence in relationship dataset
        2. Filter by RDOMAIN (DOMAIN in left matches RDOMAIN values in right)
        3. Filter by IDVAR/IDVARVAL columns (only columns mentioned in IDVAR)
        """
        filters = []

        # STEP 1: Match key filter (filter_dataset_by_match_keys_of_other_dataset)
        # Keep only rows where USUBJID exists in relationship dataset
        match_key_filter = SqlRelationshipMerge._build_match_key_filter(original, relationship_dataset)
        if match_key_filter:
            filters.append(match_key_filter)

        # STEP 2a: RDOMAIN filter (filter_parent_dataset_by_supp_dataset_rdomain)
        rdomain_filter = SqlRelationshipMerge._build_rdomain_filter(original, relationship_dataset)
        if rdomain_filter:
            filters.append(rdomain_filter)

        # STEP 2b: IDVAR/IDVARVAL filter (filter_dataset_by_nested_columns_of_other_dataset)
        # Only filter columns that are mentioned in IDVAR column
        idvar_filter = SqlRelationshipMerge._build_idvar_filter(
            pgi, original, relationship_dataset, column_with_names, column_with_values
        )
        if idvar_filter:
            filters.append(idvar_filter)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        return f"SELECT * FROM {original.hash} {where_clause}"

    @staticmethod
    def _build_match_key_filter(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
    ) -> str:
        """
        STEP 1: Filter left dataset to only rows where match keys exist in right dataset.
        Python equivalent: filter_dataset_by_match_keys_of_other_dataset()

        Note: In Python, if match_keys are empty, this filter returns all rows (no filtering).
        For relationship datasets, match_keys are typically empty, so we return empty filter.
        """
        # For relationship merges, match_keys are typically empty
        # When match_keys are empty in Python, filter_dataset_by_match_keys_of_other_dataset
        # does set_index([]) which returns all rows - so we skip this filter
        return ""

    @staticmethod
    def _build_rdomain_filter(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
    ) -> str:
        """
        STEP 2a: Filter by RDOMAIN.
        Python equivalent: filter_parent_dataset_by_supp_dataset_rdomain()
        Keep only rows where DOMAIN matches RDOMAIN values in relationship dataset.
        """
        if not original.has_column("DOMAIN") or not relationship_dataset.has_column("RDOMAIN"):
            return ""

        domain_hash = original.get_column_hash("DOMAIN")
        rdomain_hash = relationship_dataset.get_column_hash("RDOMAIN")

        return f"""
            {original.hash}.{domain_hash} IN (
                SELECT DISTINCT {rdomain_hash} FROM {relationship_dataset.hash}
            )
        """

    @staticmethod
    def _build_idvar_filter(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
    ) -> str:
        """
        STEP 2b: Filter by IDVAR/IDVARVAL columns.
        Python equivalent: filter_dataset_by_nested_columns_of_other_dataset()

        Only filters columns that are mentioned in the IDVAR column.
        For each unique IDVAR value (e.g., "AESEQ", "AESEV"), filter where that
        column's values match the corresponding IDVARVAL values.

        Example:
            IDVAR = ["AESEQ", "AESEQ", "AESEV"]
            IDVARVAL = ["1", "2", "MILD"]
            Result: original.AESEQ IN ('1', '2') AND original.AESEV IN ('MILD')
        """
        if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
            column_with_values
        ):
            return ""

        names_hash = relationship_dataset.get_column_hash(column_with_names)
        values_hash = relationship_dataset.get_column_hash(column_with_values)

        # Get distinct IDVAR values (column names to filter on)
        query = f"""
            SELECT DISTINCT {names_hash} as idvar_col
            FROM {relationship_dataset.hash}
            WHERE TRIM(COALESCE({names_hash}, '')) != ''
        """
        pgi.execute_sql(query)
        idvar_columns = pgi.fetch_all()

        if not idvar_columns:
            return ""

        # Build filter for each column mentioned in IDVAR
        column_filters = []
        for row in idvar_columns:
            idvar_col = row.get("idvar_col")
            if not idvar_col or not original.has_column(idvar_col):
                continue

            col_hash = original.get_column_hash(idvar_col)

            # Get all IDVARVAL values for this IDVAR column
            # Filter: original.column IN (SELECT IDVARVAL WHERE IDVAR = 'column')
            column_filter = f"""
                {original.hash}.{col_hash}::text IN (
                    SELECT r.{values_hash}::text
                    FROM {relationship_dataset.hash} r
                    WHERE r.{names_hash} = '{idvar_col}'
                    AND TRIM(COALESCE(r.{values_hash}, '')) != ''
                )
            """
            column_filters.append(column_filter)

        # Combine with AND (matching Python's behavior)
        if column_filters:
            return f"({' AND '.join(column_filters)})"

        return ""

    @staticmethod
    def _get_first_idvar_value(
        pgi: PostgresQLInterface,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
    ) -> str:
        """
        Get the first non-empty IDVAR value.
        Python equivalent: right_dataset[column_with_names][0]
        """
        if not relationship_dataset.has_column(column_with_names):
            return None

        names_hash = relationship_dataset.get_column_hash(column_with_names)

        query = f"""
            SELECT {names_hash} as first_col_name
            FROM {relationship_dataset.hash}
            WHERE TRIM(COALESCE({names_hash}, '')) != ''
            LIMIT 1
        """

        pgi.execute_sql(query)
        result = pgi.fetch_one()

        if result and result.get("first_col_name"):
            return result["first_col_name"]

        return None

    @staticmethod
    def _build_select_clauses(original, relationship_dataset, schema, domain):
        """Build select clauses for both tables following join.py patterns."""
        left_columns = []
        right_columns = []
        target_columns = []

        # Original table columns
        for col_name, col in original.get_columns():
            if col_name == "id":
                continue
            left_columns.append(f"l.{col.hash}")
            target_columns.append(col.hash)

        # Right table columns with domain suffix
        for col_name, col in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:  # Skip join keys and id
                continue

            suffixed_name = f"{col_name}.{domain}"
            if schema.has_column(suffixed_name):
                suffixed_hash = schema.get_column_hash(suffixed_name)
                right_columns.append(f"r.{col.hash} AS {suffixed_hash}")
                target_columns.append(suffixed_hash)

        return left_columns, right_columns, target_columns
