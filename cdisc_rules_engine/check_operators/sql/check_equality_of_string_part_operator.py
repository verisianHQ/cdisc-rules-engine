from .base_sql_operator import BaseSqlOperator


class CheckEqualityOfStringPartOperator(BaseSqlOperator):
    """Operator for checking equality of string part."""

    def execute_operator(self, other_value):
        raise NotImplementedError("check_equality_of_string_part check_operator not implemented")
