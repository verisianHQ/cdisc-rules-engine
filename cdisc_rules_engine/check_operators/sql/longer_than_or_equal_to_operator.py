from .base_sql_operator import BaseSqlOperator


class LongerThanOrEqualToOperator(BaseSqlOperator):
    """Operator for checking if value is longer than or equal to expected length."""

    def execute_operator(self, other_value):
        raise NotImplementedError("longer_than_or_equal_to check_operator not implemented")
