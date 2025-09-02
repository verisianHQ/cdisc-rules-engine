from .base_sql_operator import BaseSqlOperator


class IsOrderedSetOperator(BaseSqlOperator):
    """Operator for checking if values form an ordered set."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("is_ordered_set check_operator not implemented")
