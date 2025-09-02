from .base_sql_operator import BaseSqlOperator


class LongerThanOperator(BaseSqlOperator):
    """Operator for checking if value is longer than expected length."""

    def execute_operator(self, other_value):
        raise NotImplementedError("longer_than check_operator not implemented")
