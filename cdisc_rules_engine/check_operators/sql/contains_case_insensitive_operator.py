from .base_sql_operator import BaseSqlOperator


class ContainsCaseInsensitiveOperator(BaseSqlOperator):
    """Operator for case-insensitive contains checking."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("contains_case_insensitive check_operator not implemented")
