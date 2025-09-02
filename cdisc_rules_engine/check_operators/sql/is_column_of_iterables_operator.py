from .base_sql_operator import BaseSqlOperator


class IsColumnOfIterablesOperator(BaseSqlOperator):
    """Operator for checking if column contains iterables."""

    def execute_operator(self, other_value):
        raise NotImplementedError("is_column_of_iterables check_operator not implemented")
