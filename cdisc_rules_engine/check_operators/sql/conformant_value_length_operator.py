from .base_sql_operator import BaseSqlOperator


class ConformantValueLengthOperator(BaseSqlOperator):
    """Operator for checking if values conform to expected length."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("conformant_value_length check_operator not implemented")
