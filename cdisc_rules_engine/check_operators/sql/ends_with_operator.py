from .base_sql_operator import BaseSqlOperator


class EndsWithOperator(BaseSqlOperator):
    """Operator for checking if target ends with comparator."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = (
            self.replace_prefix(other_value.get("comparator"))
            if not value_is_literal
            else other_value.get("comparator")
        )
        comparison_data = self.get_comparator_data(comparator, value_is_literal)
        if self.validation_df.is_series(comparison_data):
            results = self.validation_df[target].astype(str).str.endswith(
                comparison_data.astype(str), na=False
            )
        else:
            results = self.validation_df[target].astype(str).str.endswith(str(comparison_data), na=False)
        return results"""
        raise NotImplementedError("ends_with check_operator not implemented")
