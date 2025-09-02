from .date_equal_to_operator import DateEqualToOperator


class DateNotEqualToOperator(DateEqualToOperator):
    """Operator for date inequality comparisons."""

    def execute_operator(self, other_value):
        """Check if target date does not equal comparator date"""
        return self._date_comparison(other_value, "!=")
