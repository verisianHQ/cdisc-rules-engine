from .base_sql_operator import BaseSqlOperator


class LongerThanOperator(BaseSqlOperator):
    """
    SQL operator that evaluates whether the length of target column values exceeds a comparator.

    This operator performs string length comparisons and handles various comparator types:

    Comparator Types:
    - Literal values: Numeric (compare length vs number) or string (compare lengths)
    - Column references: Compares lengths between columns, with automatic type detection
    - Operation variables: Supports constant-type variables with numeric or text data

    The operator automatically determines the appropriate comparison logic based on the
    comparator's data type and generates optimized SQL for per-row evaluation.

    Returns:
        Series of boolean values indicating which rows have target lengths greater than comparator
    """

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")

        # Generate cache key for the operation
        cache_key = f"{target_column}_longer_than_{str(comparator).replace(' ', '_')}"

        def sql():
            target_length = f"LENGTH(CAST({self._column_sql(target_column)} AS TEXT))"
            comparator_length = f"LENGTH(CAST({self._sql(comparator)} AS TEXT))"

            return f"""CASE WHEN {self._is_empty_sql(target_column)} THEN FALSE
                           WHEN {target_length} > {comparator_length} THEN TRUE
                           ELSE FALSE END"""

        return self._do_check_operator(cache_key, sql)
