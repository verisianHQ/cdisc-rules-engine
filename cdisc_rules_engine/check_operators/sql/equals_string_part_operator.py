from .base_sql_operator import BaseSqlOperator


class EqualsStringPartOperator(BaseSqlOperator):
    """Operator for checking if string part equals comparator."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("equals_string_part check_operator not implemented")
