from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def execute_operator(self, other_value):
        target = other_value.get("target")
        if isinstance(target, str):
            if target in self.operation_variables:
                target_var = self.operation_variables[target]
                if target_var.type == "collection":

                    def sql():
                        return f"NOT ({self._is_empty_sql(target)})"

                else:
                    raise ValueError(f"Target is an operation variable but not a collection type: {target}")
            else:
                column = self.replace_prefix(target)
                if self._exists(column):

                    def sql():
                        return self._is_empty_sql(column)

                else:

                    def sql():
                        return "FALSE"

                return self._do_check_operator(sql)
        else:
            raise ValueError(
                f"Target must not be a literal value list and must be a column or "
                f"operation collection variable: {target}"
            )
