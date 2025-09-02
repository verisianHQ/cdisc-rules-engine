from .base_sql_operator import BaseSqlOperator


class IsUniqueSetOperator(BaseSqlOperator):
    """Operator for checking if values form a unique set."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("is_unique_set check_operator not implemented")
