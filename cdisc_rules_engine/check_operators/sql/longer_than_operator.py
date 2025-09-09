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
        value_is_literal = other_value.get("value_is_literal", False)

        if value_is_literal:
            return self._handle_literal_comparator(target_column, comparator)
        elif isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif isinstance(comparator, str) and self._exists(comparator):
            return self._handle_column_comparator(target_column, comparator)
        else:
            return self._handle_invalid_comparator(target_column, comparator)

    def _handle_literal_comparator(self, target_column, comparator):
        """Handle when comparator is a literal value."""
        if isinstance(comparator, (int, float)):
            return self._create_comparison(target_column, str(int(comparator)), str(comparator), is_numeric=True)
        elif isinstance(comparator, str):
            return self._create_comparison(
                target_column, self._constant_sql(comparator), f"str_{len(comparator)}", is_numeric=False
            )
        else:
            raise ValueError(
                f"Invalid literal comparator type for longer_than operation on column '{target_column}'. "
                f"Expected numeric or string value, got {type(comparator)}."
            )

    def _handle_operation_variable_comparator(self, target_column, comparator):
        """Handle when comparator is an operation variable."""
        variable = self.operation_variables[comparator]

        if variable.type == "constant":
            constant_sql = self._constant_sql(comparator)
            is_numeric_variable = hasattr(variable, "data_type") and variable.data_type in [
                "int",
                "float",
                "number",
                "numeric",
            ]

            if is_numeric_variable:
                return self._create_comparison(target_column, constant_sql, f"opvar_{comparator}", is_numeric=True)
            else:
                return self._create_comparison(
                    target_column, f"CAST({constant_sql} AS TEXT)", f"opvar_{comparator}", is_numeric=False
                )
        else:
            raise ValueError(
                f"Unsupported operation variable type '{variable.type}' for longer_than operation "
                f"on column '{target_column}'. Expected 'constant'."
            )

    def _handle_column_comparator(self, target_column, comparator):
        """Handle when comparator is a column name."""
        comparator_column = self.replace_prefix(comparator).lower()
        comparator_col_schema = self.sql_data_service.pgi.schema.get_column(self.table_id, comparator_column)
        is_numeric_comparator = comparator_col_schema and comparator_col_schema.type == "Num"

        if is_numeric_comparator:
            return self._create_comparison(
                target_column, self._column_sql(comparator_column), comparator_column, is_numeric=True
            )
        else:
            return self._create_comparison(
                target_column,
                f"CAST({self._column_sql(comparator_column)} AS TEXT)",
                comparator_column,
                is_numeric=False,
            )

    def _handle_invalid_comparator(self, target_column, comparator):
        """Handle invalid comparator types."""
        raise ValueError(
            f"Invalid comparator type for longer_than operation on column '{target_column}'. "
            f"Expected numeric value, column name, or operation variable, got {type(comparator)}."
        )

    def _create_comparison(self, target_column, comparator_expr, cache_key_suffix, is_numeric=True):
        """Create a length comparison with appropriate cache key and SQL generation."""
        cache_key = f"{target_column}_longer_than_{cache_key_suffix}"

        def sql():
            target_length = f"LENGTH(CAST({self._column_sql(target_column)} AS TEXT))"
            if is_numeric:
                comparison = f"{target_length} > {comparator_expr}"
            else:
                comparison = f"{target_length} > LENGTH({comparator_expr})"

            return f"""CASE WHEN {comparison} THEN true ELSE false END"""

        return self._do_check_operator(cache_key, sql)
