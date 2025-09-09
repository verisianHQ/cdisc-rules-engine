from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the comparator value is a substring of the target column values.
        Returns True if the comparator is found as a substring within the target column.
        """
        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        cache_key = f"{target_column}_contains_{comparator}_{value_is_literal}_{self.case_insensitive}"

        if isinstance(comparator, str) and comparator in self.operation_variables:
            sql = self._handle_operation_variable(target_column, comparator)
        elif isinstance(comparator, list):
            sql = self._handle_literal_list(target_column, comparator)
        elif value_is_literal:
            sql = self._handle_literal_value(target_column, comparator)
        elif isinstance(comparator, str) and self._exists(comparator):
            sql = self._handle_column_reference(target_column, comparator)
        else:
            sql = self._handle_literal_value(target_column, comparator)

        return self._do_check_operator(cache_key, sql)

    def _handle_operation_variable(self, target_column, comparator):
        """Handle operation variables (constants and collections)."""
        variable = self.operation_variables[comparator]
        if variable.type == "constant":
            return self._create_constant_sql(target_column, comparator)
        elif variable.type == "collection":
            return self._create_collection_sql(target_column, comparator)
        else:
            raise ValueError(f"Unsupported operation variable type: {variable.type} " f"for variable {comparator}")

    def _handle_literal_list(self, target_column, comparator):
        """Handle literal lists (multiple values to check as substrings)."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            values_sql = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM (VALUES {values_sql}) AS list_values(value)
                          WHERE list_values.value != ''
                          AND {target_sql} LIKE '%' || list_values.value || '%'
                      )"""

        return sql

    def _handle_literal_value(self, target_column, comparator):
        """Handle single literal value case."""

        def sql():
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return sql

    def _handle_column_reference(self, target_column, comparator):
        """Handle column reference case."""

        def sql():
            comparator_sql = self._column_sql(comparator, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND NOT ({self._is_empty_sql(comparator)})
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return sql

    def _create_constant_sql(self, target_column, comparator):
        """Create SQL for constant operation variables."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return sql

    def _create_collection_sql(self, target_column, comparator):
        """Create SQL for collection operation variables."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            collection_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM {collection_sql} AS collection_values(value)
                          WHERE collection_values.value != ''
                          AND {target_sql} LIKE '%' || collection_values.value || '%'
                      )"""

        return sql
