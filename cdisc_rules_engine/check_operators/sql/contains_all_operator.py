from .base_sql_operator import BaseSqlOperator


class ContainsAllOperator(BaseSqlOperator):
    """Operator for checking if value contains all expected elements."""

    def execute_operator(self, other_value):
        raise NotImplementedError("contains_all check_operator not implemented")
