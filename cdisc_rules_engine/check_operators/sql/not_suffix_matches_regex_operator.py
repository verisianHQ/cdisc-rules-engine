from .suffix_matches_regex_operator import SuffixMatchesRegexOperator


class NotSuffixMatchesRegexOperator(SuffixMatchesRegexOperator):
    """Operator for inverted suffix regex pattern matching."""

    def execute_operator(self, other_value):
        # Get result from SuffixMatchesRegexOperator and invert it
        matches_result = super().execute_operator(other_value)
        return ~matches_result
