from .conformant_value_length_operator import ConformantValueLengthOperator


class NonConformantValueLengthOperator(ConformantValueLengthOperator):
    """Operator for checking if values do NOT conform to expected length."""

    def execute_operator(self, other_value):
        # Get result from ConformantValueLengthOperator and invert it
        conformant_result = super().execute_operator(other_value)
        return ~conformant_result
