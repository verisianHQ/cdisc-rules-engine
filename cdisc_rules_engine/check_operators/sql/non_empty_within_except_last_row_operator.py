from .empty_within_except_last_row_operator import EmptyWithinExceptLastRowOperator


class NonEmptyWithinExceptLastRowOperator(EmptyWithinExceptLastRowOperator):
    """Operator for checking if values are NOT empty within group except last row."""

    def execute_operator(self, other_value):
        result = super().execute_operator(other_value)
        return ~result
