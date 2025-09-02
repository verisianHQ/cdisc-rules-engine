from .base_sql_operator import BaseSqlOperator


class SuffixEqualToOperator(BaseSqlOperator):
    """Operator for checking if suffix equals to expected value."""

    def execute_operator(self, other_value):
        raise NotImplementedError("suffix_equal_to check_operator not implemented")
