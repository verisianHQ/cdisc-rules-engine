from .base_sql_operator import BaseSqlOperator


class VariableMetadataEqualToOperator(BaseSqlOperator):
    """Operator for checking if variable metadata equals to expected value."""

    def execute_operator(self, other_value):
        raise NotImplementedError("variable_metadata_equal_to check_operator not implemented")
