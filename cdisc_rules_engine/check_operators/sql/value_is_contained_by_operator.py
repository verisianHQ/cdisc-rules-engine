from .base_sql_operator import BaseSqlOperator


class ValueIsContainedByOperator(BaseSqlOperator):
    """Operator for checking if value is contained by another value."""

    def execute_operator(self, other_value):
        raise NotImplementedError("value_is_contained_by check_operator not implemented")
