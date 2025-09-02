from .base_sql_operator import BaseSqlOperator


class InvalidDurationOperator(BaseSqlOperator):
    """Operator for checking if duration is invalid."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("invalid_duration check_operator not implemented")
