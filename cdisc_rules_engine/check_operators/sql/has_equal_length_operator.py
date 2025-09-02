from .base_sql_operator import BaseSqlOperator


class HasEqualLengthOperator(BaseSqlOperator):
    """Operator for checking if values have equal length."""

    def execute_operator(self, other_value):
        raise NotImplementedError("has_equal_length check_operator not implemented")
