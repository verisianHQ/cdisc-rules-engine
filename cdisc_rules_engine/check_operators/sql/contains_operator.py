from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("contains check_operator not implemented")
