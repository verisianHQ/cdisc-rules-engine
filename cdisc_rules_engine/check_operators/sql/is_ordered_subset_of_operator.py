from .base_sql_operator import BaseSqlOperator


class IsOrderedSubsetOfOperator(BaseSqlOperator):
    """Operator for checking if value is an ordered subset of another value."""

    def execute_operator(self, other_value):
        raise NotImplementedError("is_ordered_subset_of check_operator not implemented")
