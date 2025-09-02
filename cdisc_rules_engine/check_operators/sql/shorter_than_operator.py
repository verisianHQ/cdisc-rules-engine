from .base_sql_operator import BaseSqlOperator


class ShorterThanOperator(BaseSqlOperator):
    """Operator for checking if value is shorter than expected length."""

    def execute_operator(self, other_value):
        """return ~self.longer_than_or_equal_to(other_value)"""
        raise NotImplementedError("shorter_than check_operator not implemented")
