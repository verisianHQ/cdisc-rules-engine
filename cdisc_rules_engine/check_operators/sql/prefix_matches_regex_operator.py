from .base_sql_operator import BaseSqlOperator


class PrefixMatchesRegexOperator(BaseSqlOperator):
    """Operator for prefix regex pattern matching."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError
        raise NotImplementedError("prefix_matches_regex check_operator not implemented")
