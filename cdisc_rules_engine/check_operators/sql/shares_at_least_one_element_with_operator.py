from .base_sql_operator import BaseSqlOperator


class SharesAtLeastOneElementWithOperator(BaseSqlOperator):
    """Operator for checking if values share at least one element."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        if not value_is_literal:
            comparator = self.replace_prefix(comparator)
        comparator_column = (
            self._column_sql(comparator) if not value_is_literal else self._sql(comparator, value_is_literal=True)
        )
        operator_name = f"{target}_shares_at_least_one_element_with_{comparator}"

        def sql():
            target_array = f"string_to_array({target_column}::text, ',')"
            comparator_array = f"string_to_array({comparator_column}::text, ',')"

            return f"""CASE
                    WHEN {self._is_empty_sql(target)} THEN FALSE
                    WHEN {comparator_column} IS NULL THEN FALSE
                    WHEN {target_array} && {comparator_array} THEN TRUE
                    ELSE FALSE
                    END"""

        return self._do_check_operator(operator_name, sql)
