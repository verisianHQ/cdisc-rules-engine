from .less_than_operator import LessThanOperator


class LessThanOrEqualToOperator(LessThanOperator):
    """Operator for numeric less-than-or-equal-to comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, "<=")
