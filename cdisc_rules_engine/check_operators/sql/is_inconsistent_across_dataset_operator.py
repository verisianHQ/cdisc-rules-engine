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
        where_populated = other_value.get("where_populated", False)

        if not target or not isinstance(target, str) or target in self.operation_variables:
            raise ValueError("Target is required and must be a valid column name.")

        target_column = self.replace_prefix(target).lower()
        if not self._exists(target_column):
            return self._do_check_operator(lambda: "FALSE")

        if isinstance(comparator, str):
            comparator_list = [comparator]
        elif isinstance(comparator, list):
            comparator_list = comparator
        else:
            raise ValueError(
                f"Invalid comparator type for is_inconsistent_across_dataset operation on column '{target_column}'. "
                f"Expected string or list of column names, got: {type(comparator).__name__}"
            )

        valid_comparators = []
        for comp in comparator_list:
            comp_col = self.replace_prefix(comp).lower()
            if self._exists(comp_col):
                valid_comparators.append(comp_col)

        if len(valid_comparators) == 0:
            return self._do_check_operator(lambda: "FALSE")
        elif len(valid_comparators) == 1:
            return self._handle_single_comparator(target_column, valid_comparators[0], where_populated)
        else:
            return self._handle_multiple_comparators(target_column, valid_comparators, where_populated)

    def _handle_single_comparator(self, target_column, comparator_column, where_populated=False):
        cache_key = f"{target_column}_inconsistent_across_{comparator_column}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            populated_filter = ""
            current_row_filter = ""
            if where_populated:
                populated_filter = (
                    f" AND t2.{self._column_sql(target_column, alias=False)} IS NOT NULL"
                    f" AND t2.{self._column_sql(comparator_column, alias=False)} IS NOT NULL"
                )
                current_row_filter = (
                    f" AND t1.{self._column_sql(target_column, alias=False)} IS NOT NULL"
                    f" AND t1.{self._column_sql(comparator_column, alias=False)} IS NOT NULL"
                )
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
                            ){populated_filter}
                        ) > 1{current_row_filter} AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)

    def _handle_multiple_comparators(self, target_column, comparator_columns, where_populated=False):
        cache_key = f"{target_column}_inconsistent_across_{'_'.join(comparator_columns)}"

        def generate_update_query(db_table: str, db_column: str) -> str:
            where_conditions = []
            for comp_col in comparator_columns:
                condition = (
                    f"(t2.{self._column_sql(comp_col, alias=False)} = t1.{self._column_sql(comp_col, alias=False)}) "
                    f"OR (t2.{self._column_sql(comp_col, alias=False)} IS NULL "
                    f"  AND t1.{self._column_sql(comp_col, alias=False)} IS NULL)"
                )
                where_conditions.append(f"({condition})")
            where_clause = " AND ".join(where_conditions)
            current_row_filter = ""
            if where_populated:
                populated_columns = [target_column, *comparator_columns]
                where_clause += " AND " + " AND ".join(
                    f"t2.{self._column_sql(column, alias=False)} IS NOT NULL" for column in populated_columns
                )
                current_row_filter = " AND " + " AND ".join(
                    f"t1.{self._column_sql(column, alias=False)} IS NOT NULL" for column in populated_columns
                )

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
                        ) > 1 {current_row_filter} AS is_inconsistent
                    FROM {db_table} AS t1
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(cache_key, generate_update_query)
