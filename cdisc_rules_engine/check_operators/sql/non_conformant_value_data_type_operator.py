from .conformant_value_data_type_operator import ConformantValueDataTypeOperator


class NonConformantValueDataTypeOperator(ConformantValueDataTypeOperator):
    """Operator for checking if values do NOT conform to expected data type."""

    def execute_operator(self, other_value):
        # Get result from ConformantValueDataTypeOperator and invert it
        conformant_result = super().execute_operator(other_value)
        return ~conformant_result
