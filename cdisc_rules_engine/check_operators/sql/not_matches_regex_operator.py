from .matches_regex_operator import MatchesRegexOperator


class NotMatchesRegexOperator(MatchesRegexOperator):
    """Operator for inverted regex pattern matching."""

    def execute_operator(self, other_value):
        # Get result from MatchesRegexOperator and invert it
        matches_result = super().execute_operator(other_value)
        return ~matches_result
