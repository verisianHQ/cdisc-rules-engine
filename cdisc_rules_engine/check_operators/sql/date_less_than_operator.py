from .date_equal_to_operator import DateEqualToOperator


class DateLessThanOperator(DateEqualToOperator):
    """Operator for date less-than comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is less than comparator date"""
        return self._date_comparison(other_value, "<")
