from .date_equal_to_operator import DateEqualToOperator


class DateGreaterThanOperator(DateEqualToOperator):
    """Operator for date greater-than comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is greater than comparator date"""
        return self._date_comparison(other_value, ">")
