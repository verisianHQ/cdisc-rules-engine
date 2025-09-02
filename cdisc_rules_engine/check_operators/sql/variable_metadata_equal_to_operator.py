from .base_sql_operator import BaseSqlOperator


class VariableMetadataEqualToOperator(BaseSqlOperator):
    """Operator for checking if variable metadata equals to expected value."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        metadata_type = other_value.get("metadata_type")

        # Check if target column has the expected metadata value
        if metadata_type in self.value_level_metadata:
            metadata_values = self.value_level_metadata[metadata_type]
            if target in metadata_values:
                expected_value = metadata_values[target]
                results = pd.Series([expected_value == comparator] * len(self.validation_df))
            else:
                results = pd.Series([False] * len(self.validation_df))
        else:
            results = pd.Series([False] * len(self.validation_df))

        return results"""
        raise NotImplementedError("variable_metadata_equal_to check_operator not implemented")
