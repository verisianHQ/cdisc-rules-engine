from .base_sql_operator import BaseSqlOperator


class IsInconsistentAcrossDatasetOperator(BaseSqlOperator):
    """Operator for checking if values are inconsistent across dataset."""

    def execute_operator(self, other_value):
        """
        Checks if values in the target column are inconsistent across groups defined by comparator column(s).

        Returns True for rows where the target column has multiple distinct values within the same group,
        False for rows where all values in the group are consistent.
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        target_column = self._get_valid_target(target)
        if not target_column:
            return self._do_check_operator(lambda: "FALSE")

        if not isinstance(comparator, (str, list)):
            raise ValueError(
                f"Invalid comparator type for is_inconsistent_across_dataset operation on column '{target_column}'. "
                f"Expected string or list of column names, got: {type(comparator).__name__}"
            )

        valid_comparators = self._get_valid_comparators(comparator)

        if not valid_comparators:
            return self._handle_constant_grouping(target_column)
        elif len(valid_comparators) == 1:
            return self._handle_single_comparator(target_column, valid_comparators[0])
        else:
            return self._handle_multiple_comparators(target_column, valid_comparators)

    def _get_valid_target(self, target) -> str | None:
        if isinstance(target, list) or (isinstance(target, str) and target in self.operation_variables):
            return None

        target_column = self.replace_prefix(target)
        if not target_column or not self._exists(target_column.lower()):
            return None

        return target_column.lower()

    def _get_valid_comparators(self, comparator) -> list:
        if isinstance(comparator, str):
            if comparator in self.operation_variables:
                return []
            comp_column = self.replace_prefix(comparator).lower()
            if not self._exists(comp_column):
                return []
            return [comp_column]

        valid_comp_columns = []
        for comp in comparator:
            if isinstance(comp, str) and comp not in self.operation_variables:
                comp_col = self.replace_prefix(comp).lower()
                if self._exists(comp_col):
                    valid_comp_columns.append(comp_col)

        return valid_comp_columns

    def _handle_constant_grouping(self, target_column):
        cache_key = f"{target_column}_inconsistent_across_global"

        def generate_update_query(db_table: str, db_column: str) -> str:
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_inconsistent
                FROM (
                    SELECT
                        id,
                        (
                            SELECT COUNT(DISTINCT
                                CASE
                                    WHEN {self._column_sql(target_column, alias=False)} IS NULL THEN 'NULL_VALUE'
                                    ELSE CAST({self._column_sql(target_column, alias=False)} AS TEXT)
                                END
                            )
                            FROM {db_table}
                        ) > 1 AS is_inconsistent
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)

    def _handle_single_comparator(self, target_column, comparator_column):
        cache_key = f"{target_column}_inconsistent_across_{comparator_column}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_inconsistent
                FROM (
                    SELECT
                        id,
                        (
                            SELECT COUNT(DISTINCT
                                CASE
                                    WHEN t2.{self._column_sql(target_column, alias=False)} IS NULL THEN 'NULL_VALUE'
                                    ELSE CAST(t2.{self._column_sql(target_column, alias=False)} AS TEXT)
                                END
                            )
                            FROM {db_table} AS t2
                            WHERE (
                                (t2.{self._column_sql(comparator_column, alias=False)}
                                    = t1.{self._column_sql(comparator_column, alias=False)})
                                OR
                                (t2.{self._column_sql(comparator_column, alias=False)} IS NULL
                                AND
                                t1.{self._column_sql(comparator_column, alias=False)} IS NULL)
                            )
                        ) > 1 AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)

    def _handle_multiple_comparators(self, target_column, comparator_columns):
        cache_key = f"{target_column}_inconsistent_across_{'_'.join(comparator_columns)}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            # Build the WHERE clause for matching groups, handling NULLs properly
            where_conditions = []
            for comp_col in comparator_columns:
                condition = (
                    f"(t2.{self._column_sql(comp_col, alias=False)} = t1.{self._column_sql(comp_col, alias=False)}) "
                    f"OR (t2.{self._column_sql(comp_col, alias=False)} IS NULL "
                    f"  AND t1.{self._column_sql(comp_col, alias=False)} IS NULL)"
                )
                where_conditions.append(f"({condition})")
            where_clause = " AND ".join(where_conditions)

            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_inconsistent
                FROM (
                    SELECT
                        id,
                        (
                            SELECT COUNT(DISTINCT
                                CASE
                                    WHEN t2.{self._column_sql(target_column, alias=False)} IS NULL THEN 'NULL_VALUE'
                                    ELSE CAST(t2.{self._column_sql(target_column, alias=False)} AS TEXT)
                                END
                            )
                            FROM {db_table} AS t2
                            WHERE {where_clause}
                        ) > 1 AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)
