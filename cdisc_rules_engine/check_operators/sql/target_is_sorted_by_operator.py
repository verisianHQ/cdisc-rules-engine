from .base_sql_operator import BaseSqlOperator


class TargetIsSortedByOperator(BaseSqlOperator):
    """Operator for checking if target is sorted by specified criteria."""

    def execute_operator(self, other_value):
        raise NotImplementedError("target_is_sorted_by check_operator not implemented")
