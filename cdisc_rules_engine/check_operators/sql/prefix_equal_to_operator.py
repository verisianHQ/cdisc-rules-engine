from .base_sql_operator import BaseSqlOperator


class PrefixEqualToOperator(BaseSqlOperator):
    """Operator for checking if prefix equals to expected value."""

    def execute_operator(self, other_value):
        raise NotImplementedError("prefix_equal_to check_operator not implemented")
