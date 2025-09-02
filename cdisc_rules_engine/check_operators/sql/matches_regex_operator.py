from .base_sql_operator import BaseSqlOperator


class MatchesRegexOperator(BaseSqlOperator):
    """Operator for regex pattern matching."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("matches_regex check_operator not implemented")
