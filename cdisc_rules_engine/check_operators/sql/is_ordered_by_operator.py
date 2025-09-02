from .base_sql_operator import BaseSqlOperator


class IsOrderedByOperator(BaseSqlOperator):
    """Operator for checking if data is ordered by specified columns."""

    def execute_operator(self, other_value):
        raise NotImplementedError("is_ordered_by check_operator not implemented")
