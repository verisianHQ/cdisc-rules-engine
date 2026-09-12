from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the comparator value is a substring of the target column values.
        Also handles cases where the target is a collection operation variable or a literal value list.
        Returns True if the comparator is found as a substring within the target column.
        """
        target = other_value.get("target")
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        is_target_list = isinstance(target, list)
        is_target_collection = (
            isinstance(target, str)
            and target in self.operation_variables
            and self.operation_variables[target].type == "collection"
        )

        if is_target_list or is_target_collection:
            return self._handle_array_target(target, comparator, value_is_literal, is_target_list)

        target_sql, target_empty_sql = self._get_scalar_target_sql(target)
        return self._handle_scalar_target(target_sql, target_empty_sql, target, comparator, value_is_literal)

    def _get_scalar_target_sql(self, target):
        target_name = self.replace_prefix(target)

        if self._exists(target_name):
            target_sql = self._column_sql(target_name, lowercase=self.case_insensitive)
            target_empty_sql = self._is_empty_sql(target_name)
        else:
            target_sql = self._constant_sql(target_name, lowercase=self.case_insensitive)
            target_empty_sql = "FALSE" if target_name else "TRUE"

        return target_sql, target_empty_sql

    def _handle_scalar_target(self, target_sql, target_empty_sql, target_raw, comparator, value_is_literal):
        if isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_sql, target_empty_sql, comparator)
        elif isinstance(comparator, list):
            if not comparator:
                return self._do_check_operator(lambda: "FALSE")
            return self._handle_list_comparator(target_sql, target_empty_sql, comparator)
        elif isinstance(comparator, str) and not value_is_literal and self._exists(self.replace_prefix(comparator)):
            return self._handle_column_comparator(target_sql, target_empty_sql, comparator)
        elif isinstance(comparator, str):
            return self._handle_literal_value(target_sql, target_empty_sql, comparator)
        else:
            target_name = self.replace_prefix(target_raw)
            raise ValueError(
                f"Invalid comparator type for contains operation on column '{target_name}'. "
                f"Expected list, column name, or operation variable, but got: {type(comparator).__name__}"
            )

    def _handle_array_target(self, target, comparator, value_is_literal, is_target_list):
        if is_target_list:
            if not target:
                return self._do_check_operator(lambda: "FALSE")
            values = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in target)
            target_table_sql = f"(VALUES {values})"
        else:
            target_table_sql = self._collection_sql(target, lowercase=self.case_insensitive)

        def sql():
            if value_is_literal:
                comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            else:
                comparator_sql = self._sql(
                    comparator, lowercase=self.case_insensitive, value_is_literal=value_is_literal
                )

            return f"""EXISTS (
                        SELECT 1 FROM {target_table_sql} AS collection_values(value)
                        WHERE collection_values.value = {comparator_sql}
                    )"""

        return self._do_check_operator(sql)

    def _handle_list_comparator(self, target_sql, target_empty_sql, comparator):
        def sql():
            values_sql = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            return f"""NOT ({target_empty_sql})
                      AND EXISTS (
                          SELECT 1 FROM (VALUES {values_sql}) AS list_values(value)
                          WHERE list_values.value != ''
                          AND {target_sql} LIKE '%' || list_values.value || '%'
                      )"""

        return self._do_check_operator(sql)

    def _handle_operation_variable_comparator(self, target_sql, target_empty_sql, comparator):
        variable = self.operation_variables[comparator]

        if variable.type == "constant":

            def sql():
                comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
                return f"""NOT ({target_empty_sql})
                          AND {comparator_sql} != ''
                          AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

            return self._do_check_operator(sql)

        elif variable.type == "collection":

            def sql():
                collection_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
                return f"""NOT ({target_empty_sql})
                          AND EXISTS (
                              SELECT 1 FROM {collection_sql} AS collection_values(value)
                              WHERE collection_values.value != ''
                              AND {target_sql} LIKE '%' || collection_values.value || '%'
                          )"""

            return self._do_check_operator(sql)

        else:
            raise ValueError(
                f"Unsupported operation variable type: {variable.type} "
                f"for variable {comparator}. Expected 'collection' or 'constant'."
            )

    def _handle_column_comparator(self, target_sql, target_empty_sql, comparator):
        comparator_column = self.replace_prefix(comparator)

        def sql():
            comparator_sql = self._column_sql(comparator_column, lowercase=self.case_insensitive)
            return f"""NOT ({target_empty_sql})
                      AND NOT ({self._is_empty_sql(comparator_column)})
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_literal_value(self, target_sql, target_empty_sql, comparator):
        def sql():
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({target_empty_sql})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(sql)
