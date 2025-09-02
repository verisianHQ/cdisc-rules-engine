from .base_sql_operator import BaseSqlOperator


class EndsWithOperator(BaseSqlOperator):
    """Operator for checking if target ends with comparator."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("ends_with check_operator not implemented")
