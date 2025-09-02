from .base_sql_operator import BaseSqlOperator


class StartsWithOperator(BaseSqlOperator):
    """Operator for checking if target starts with comparator."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("starts_with check_operator not implemented")
