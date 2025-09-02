from .prefix_matches_regex_operator import PrefixMatchesRegexOperator


class NotPrefixMatchesRegexOperator(PrefixMatchesRegexOperator):
    """Operator for inverted prefix regex pattern matching."""

    def execute_operator(self, other_value):
        # Get result from PrefixMatchesRegexOperator and invert it
        matches_result = super().execute_operator(other_value)
        return ~matches_result
