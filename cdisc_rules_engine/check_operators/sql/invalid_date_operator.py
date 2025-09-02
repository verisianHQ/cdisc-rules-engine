from .base_sql_operator import BaseSqlOperator


class InvalidDateOperator(BaseSqlOperator):
    """Operator for checking if date is invalid."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("invalid_date check_operator not implemented")
