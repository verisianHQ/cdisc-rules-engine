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

        # Validate columns exist in relationship dataset
        if not relationship_dataset.has_column(column_with_names):
            raise ValueError(f"RELATIONSHIP MERGE: Right (relationship) schema missing column: {column_with_names}")

        if not relationship_dataset.has_column(column_with_values):
            raise ValueError(f"RELATIONSHIP MERGE: Right (relationship) schema missing column: {column_with_values}")

        # Check for DOMAIN/RDOMAIN compatibility - allow more flexible combinations for CG0371
        # The validation should allow: DOMAIN-RDOMAIN, RDOMAIN-RDOMAIN, etc.
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
            # If column names are null/empty, treat as empty relationship columns
            return True

        # Validate columns exist before getting their hashes
        if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
            column_with_values
        ):
            # If required columns don't exist, treat as empty relationship columns
            return True

        try:
            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            # Add null safety for hash values
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
            # If any error occurs, treat as empty relationship columns to prevent crashes
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
        Execute the relationship merge using single query that replicates Python logic.

        This follows the Python merge_relationship_datasets process:
        1. Filter by match keys + RDOMAIN + relationship columns
        2. Full outer join with domain suffixes
        """
        # Build select clauses for both tables
        left_columns, right_columns, target_columns = SqlRelationshipMerge._build_select_clauses(
            original, relationship_dataset, schema, domain
        )

        # Check if relationship columns are empty - if so, do simple outer join
        if SqlRelationshipMerge._has_empty_relationship_columns(
            pgi, relationship_dataset, column_with_names, column_with_values
        ):
            # Simple outer join when no relationship data
            base_join_conditions = SqlRelationshipMerge._build_join_conditions(original, relationship_dataset)
            filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
                original, relationship_dataset, column_with_names, column_with_values
            )
            query = f"""
                INSERT INTO {schema.hash} ({', '.join(target_columns)})
                SELECT {', '.join(left_columns + right_columns)}
                FROM ({filtered_left_query}) l
                FULL OUTER JOIN {relationship_dataset.hash} r ON {' AND '.join(base_join_conditions)}
            """
        else:
            # Complex join with relationship column matching (replicates exact Python logic)
            # Python: left_ds_col_name: str = right_dataset[column_with_names][0]
            # Python: left_dataset_match_keys.append(left_ds_col_name)
            # Python: right_dataset_match_keys.append(column_with_values)

            # This means join on: STUDYID, USUBJID, and first_column_name = IDVARVAL
            # Get first non-empty value from column_with_names to determine the join column
            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            # Query to get the first non-empty column name (replicating Python's [0] access)
            pgi.execute_sql(
                f"""
                SELECT {names_hash} as first_col_name
                FROM {relationship_dataset.hash}
                WHERE TRIM(COALESCE({names_hash}, '')) != ''
                LIMIT 1
            """
            )
            result = pgi.fetch_one()

            if result and result.get("first_col_name"):
                first_col_name = result["first_col_name"]

                # Check if this column exists in original dataset
                if original.has_column(first_col_name):
                    col_hash = original.get_column_hash(first_col_name)

                    filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
                        original, relationship_dataset, column_with_names, column_with_values
                    )

                    # Build the exact join that Python does:
                    # LEFT: STUDYID, USUBJID, ECSEQ (first_col_name)
                    # RIGHT: STUDYID, USUBJID, IDVARVAL (column_with_values)
                    left_studyid = original.get_column_hash("STUDYID")
                    right_studyid = relationship_dataset.get_column_hash("STUDYID")
                    left_usubjid = original.get_column_hash("USUBJID")
                    right_usubjid = relationship_dataset.get_column_hash("USUBJID")

                    studyid_join = f"l.{left_studyid} = r.{right_studyid}"
                    usubjid_join = f"l.{left_usubjid} = r.{right_usubjid}"
                    dynamic_join = f"l.{col_hash}::text = r.{values_hash}"

                    query = f"""
                        INSERT INTO {schema.hash} ({', '.join(target_columns)})
                        SELECT {', '.join(left_columns + right_columns)}
                        FROM ({filtered_left_query}) l
                        FULL OUTER JOIN {relationship_dataset.hash} r ON (
                            {studyid_join}
                            AND {usubjid_join}
                            AND {dynamic_join}
                        )
                    """
                else:
                    # Column doesn't exist, do simple join without the dynamic column
                    filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
                        original, relationship_dataset, column_with_names, column_with_values
                    )

                    left_studyid = original.get_column_hash("STUDYID")
                    right_studyid = relationship_dataset.get_column_hash("STUDYID")
                    left_usubjid = original.get_column_hash("USUBJID")
                    right_usubjid = relationship_dataset.get_column_hash("USUBJID")

                    studyid_join = f"l.{left_studyid} = r.{right_studyid}"
                    usubjid_join = f"l.{left_usubjid} = r.{right_usubjid}"

                    query = f"""
                        INSERT INTO {schema.hash} ({', '.join(target_columns)})
                        SELECT {', '.join(left_columns + right_columns)}
                        FROM ({filtered_left_query}) l
                        FULL OUTER JOIN {relationship_dataset.hash} r ON (
                            {studyid_join}
                            AND {usubjid_join}
                        )
                    """
            else:
                # No valid column names found, do simple join
                filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
                    original, relationship_dataset, column_with_names, column_with_values
                )

                left_studyid = original.get_column_hash("STUDYID")
                right_studyid = relationship_dataset.get_column_hash("STUDYID")
                left_usubjid = original.get_column_hash("USUBJID")
                right_usubjid = relationship_dataset.get_column_hash("USUBJID")

                studyid_join = f"l.{left_studyid} = r.{right_studyid}"
                usubjid_join = f"l.{left_usubjid} = r.{right_usubjid}"

                query = f"""
                    INSERT INTO {schema.hash} ({', '.join(target_columns)})
                    SELECT {', '.join(left_columns + right_columns)}
                    FROM ({filtered_left_query}) l
                    FULL OUTER JOIN {relationship_dataset.hash} r ON (
                        {studyid_join}
                        AND {usubjid_join}
                    )
                """

        pgi.execute_sql(query)

    @staticmethod
    def _build_filtered_left_subquery(original, relationship_dataset, column_with_names, column_with_values):
        """
        Build subquery that replicates Python filtering steps.
        For relationship merges, we want ALL records from original dataset
        (unlike other merge types that filter by match keys).
        """
        filters = []

        # Step 2: RDOMAIN filtering (if present)
        if original.has_column("DOMAIN") and relationship_dataset.has_column("RDOMAIN"):
            domain_hash = original.get_column_hash("DOMAIN")
            rdomain_hash = relationship_dataset.get_column_hash("RDOMAIN")
            filters.append(
                f"""
                {original.hash}.{domain_hash} IN (
                    SELECT DISTINCT {rdomain_hash} FROM {relationship_dataset.hash}
                )
            """
            )

        # Step 3: Relationship columns filtering (if both columns exist and have non-empty values)
        if relationship_dataset.has_column(column_with_names) and relationship_dataset.has_column(column_with_values):
            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            # Build filtering for each column in original that has matching relationship data
            relationship_filters = []
            for col_name, _ in original.get_columns():
                if col_name in ["id", "STUDYID", "USUBJID", "DOMAIN"]:
                    continue

                col_hash = original.get_column_hash(col_name)
                # If this column has relationship data, filter by it (keep only matching values)
                # Otherwise, keep all rows for this column
                relationship_filters.append(
                    f"""
                    (
                        NOT EXISTS (
                            SELECT 1 FROM {relationship_dataset.hash} r_check
                            WHERE r_check.{names_hash} = '{col_name}'
                            AND TRIM(COALESCE(r_check.{names_hash}, '')) != ''
                        )
                        OR
                        {original.hash}.{col_hash}::text IN (
                            SELECT r_rel.{values_hash}::text
                            FROM {relationship_dataset.hash} r_rel
                            WHERE r_rel.{names_hash} = '{col_name}'
                            AND TRIM(COALESCE(r_rel.{names_hash}, '')) != ''
                            AND TRIM(COALESCE(r_rel.{values_hash}, '')) != ''
                        )
                    )
                """
                )

            if relationship_filters:
                filters.append(f"({' AND '.join(relationship_filters)})")

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        return f"SELECT * FROM {original.hash} {where_clause}"

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

    @staticmethod
    def _build_join_conditions(original, relationship_dataset):
        """Build join conditions for STUDYID and USUBJID."""
        return [
            f"l.{original.get_column_hash('STUDYID')} = r.{relationship_dataset.get_column_hash('STUDYID')}",
            f"l.{original.get_column_hash('USUBJID')} = r.{relationship_dataset.get_column_hash('USUBJID')}",
        ]
