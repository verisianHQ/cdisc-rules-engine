from typing import List

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
        Perform a relationship merge operation following SUPP patterns.

        This is the SQL equivalent of merge_relationship_datasets() which:
        1. Filter original by match keys of relationship dataset
        2. Filter by RDOMAIN (like SUPP filtering)
        3. Filter by relationship columns (nested column filtering)
        4. Merge with outer join and domain suffix
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

            # Build the merged schema
            schema = SqlRelationshipMerge._build_merged_schema(pgi, name, original, relationship_dataset, domain)
            pgi.create_table(schema)

            # Execute the merge using batch operations like SUPP
            queries = SqlRelationshipMerge._build_merge_queries(
                pgi, schema, original, relationship_dataset, domain, column_with_names, column_with_values
            )

            # Only execute queries if we have valid queries to execute
            if queries:
                pgi.execute_many(queries)

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
            raise ValueError(f"RELATIONSHIP MERGE: Right schema missing column: {column_with_names}")

        if not relationship_dataset.has_column(column_with_values):
            raise ValueError(f"RELATIONSHIP MERGE: Right schema missing column: {column_with_values}")

        # Check for DOMAIN/RDOMAIN if present
        if relationship_dataset.has_column("RDOMAIN") and not original.has_column("DOMAIN"):
            raise ValueError("RELATIONSHIP MERGE: Original schema missing DOMAIN column when right has RDOMAIN")

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
        schema = SqlRelationshipMerge._build_merged_schema(pgi, name, original, relationship_dataset, domain)
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
        pgi: PostgresQLInterface,
        name: str,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
    ) -> SqlTableSchema:
        """Build merged schema following SUPP patterns."""
        schema = SqlTableSchema.from_join(name)

        # Add all original columns (like SUPP)
        for col_name, column in original.get_columns():
            if col_name == "id":
                continue
            schema.add_column(column)

        # Add relationship dataset columns with domain suffix
        for col_name, column in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:  # Skip these
                continue

            suffixed_name = f"{col_name}.{domain}"
            if not schema.has_column(suffixed_name):
                new_col_schema = SqlColumnSchema.generated(column=suffixed_name, type=column.type)
                schema.add_column(new_col_schema)

        return schema

    @staticmethod
    def _append_initial_copy_query(queries: List[str], schema: SqlTableSchema, original: SqlTableSchema):
        """Step 1: Copy all original data into the new merged schema."""
        orig_columns = [col.hash for col_name, col in original.get_columns() if col_name != "id"]
        queries.append(
            f"""
            INSERT INTO {schema.hash} ({', '.join(orig_columns)})
            SELECT {', '.join(orig_columns)}
            FROM {original.hash}
        """
        )

    @staticmethod
    def _append_match_key_filter_query(
        queries: List[str], schema: SqlTableSchema, relationship_dataset: SqlTableSchema
    ):
        """Step 2: Filter by match keys of the relationship dataset."""
        studyid_hash = schema.get_column_hash("STUDYID")
        usubjid_hash = schema.get_column_hash("USUBJID")
        rel_studyid_hash = relationship_dataset.get_column_hash("STUDYID")
        rel_usubjid_hash = relationship_dataset.get_column_hash("USUBJID")

        queries.append(
            f"""
            DELETE FROM {schema.hash}
            WHERE NOT EXISTS (
                SELECT 1 FROM {relationship_dataset.hash} r
                WHERE {schema.hash}.{studyid_hash} = r.{rel_studyid_hash}
                AND {schema.hash}.{usubjid_hash} = r.{rel_usubjid_hash}
            )
        """
        )

    @staticmethod
    def _append_rdomain_filter_query(queries: List[str], schema: SqlTableSchema, relationship_dataset: SqlTableSchema):
        """Step 3: Filter by RDOMAIN if present."""
        domain_hash = schema.get_column_hash("DOMAIN")
        rdomain_hash = relationship_dataset.get_column_hash("RDOMAIN")

        queries.append(
            f"""
            DELETE FROM {schema.hash}
            WHERE {schema.hash}.{domain_hash} NOT IN (
                SELECT DISTINCT {rdomain_hash}
                FROM {relationship_dataset.hash}
            )
        """
        )

    @staticmethod
    def _append_relationship_column_filter_queries(
        queries: List[str],
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        names_hash: str,
        values_hash: str,
    ):
        """Step 4: Filter by the contents of the relationship columns."""
        for col_name, _ in original.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID", "DOMAIN"]:
                continue

            col_hash = schema.get_column_hash(col_name)
            queries.append(
                f"""
                DELETE FROM {schema.hash}
                WHERE {schema.hash}.{col_hash}::text NOT IN (
                    SELECT r.{values_hash}::text
                    FROM {relationship_dataset.hash} r
                    WHERE r.{names_hash} = '{col_name}'
                    AND TRIM(COALESCE(r.{names_hash}, '')) != ''
                    AND TRIM(COALESCE(r.{values_hash}, '')) != ''
                )
                AND EXISTS (
                    SELECT 1 FROM {relationship_dataset.hash} r2
                    WHERE r2.{names_hash} = '{col_name}'
                    AND TRIM(COALESCE(r2.{names_hash}, '')) != ''
                )
            """
            )

    @staticmethod
    def _append_final_update_queries(
        queries: List[str],
        schema: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        relationship_col: str,
        values_hash: str,
    ):
        """Step 5: Perform the final outer join merge via UPDATE statements."""
        for col_name, _ in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:
                continue

            suffixed_name = f"{col_name}.{domain}"
            if schema.has_column(suffixed_name):
                # Validate all required hashes before building query
                if all(
                    [
                        relationship_dataset.has_column(col_name),
                        schema.has_column("STUDYID"),
                        schema.has_column("USUBJID"),
                        relationship_dataset.has_column("STUDYID"),
                        relationship_dataset.has_column("USUBJID"),
                        schema.has_column(relationship_col),
                    ]
                ):
                    orig_studyid_hash = schema.get_column_hash("STUDYID")
                    orig_usubjid_hash = schema.get_column_hash("USUBJID")
                    orig_rel_col_hash = schema.get_column_hash(relationship_col)
                    rel_studyid_hash = relationship_dataset.get_column_hash("STUDYID")
                    rel_usubjid_hash = relationship_dataset.get_column_hash("USUBJID")
                    queries.append(
                        f"""
                        UPDATE {schema.hash} AS orig
                        SET {schema.get_column_hash(suffixed_name)} = r.{relationship_dataset.get_column_hash(col_name)}
                        FROM {relationship_dataset.hash} AS r
                        WHERE orig.{orig_studyid_hash} = r.{rel_studyid_hash}
                        AND orig.{orig_usubjid_hash} = r.{rel_usubjid_hash}
                        AND orig.{orig_rel_col_hash}::text = r.{values_hash}::text
                    """
                    )

    @staticmethod
    def _build_merge_queries(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        column_with_names: str,
        column_with_values: str,
    ) -> List[str]:
        """Build queries following SUPP batch pattern by calling helper methods."""
        queries = []

        SqlRelationshipMerge._append_initial_copy_query(queries, schema, original)
        SqlRelationshipMerge._append_match_key_filter_query(queries, schema, relationship_dataset)

        if relationship_dataset.has_column("RDOMAIN") and original.has_column("DOMAIN"):
            SqlRelationshipMerge._append_rdomain_filter_query(queries, schema, relationship_dataset)

        try:
            if not column_with_names or not column_with_values:
                return queries
            if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
                column_with_values
            ):
                return queries

            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            if not names_hash or not values_hash:
                return queries

            SqlRelationshipMerge._append_relationship_column_filter_queries(
                queries, schema, original, relationship_dataset, names_hash, values_hash
            )

            pgi.execute_sql(
                f"SELECT {names_hash} as col FROM {relationship_dataset.hash} WHERE {names_hash} IS NOT NULL LIMIT 1"
            )
            first_col_result = pgi.fetch_one()

            if first_col_result and first_col_result.get("col") and schema.has_column(first_col_result["col"]):
                relationship_col = first_col_result["col"]
                SqlRelationshipMerge._append_final_update_queries(
                    queries, schema, relationship_dataset, domain, relationship_col, values_hash
                )
        except Exception:
            pass

        return queries
