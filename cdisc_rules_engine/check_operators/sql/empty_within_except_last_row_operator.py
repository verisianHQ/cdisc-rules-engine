from .base_sql_operator import BaseSqlOperator


class EmptyWithinExceptLastRowOperator(BaseSqlOperator):
    """Operator for checking if values are empty within group except last row."""

    def execute_operator(self, other_value):
        raise NotImplementedError("empty_within_except_last_row check_operator not implemented")
