from .base_sql_operator import BaseSqlOperator


class PrefixIsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target prefix is contained by the comparator."""

    def execute_operator(self, other_value):
        """
        Checks if target prefix is contained by the comparator.
        """
        target = self.replace_prefix(other_value.get("target"))
        prefix_length = other_value.get("prefix")
        prefix_sql = f"LEFT({target}, {prefix_length})"

        # Create new other_value with the prefix SQL as target
        modified_other_value = other_value.copy()
        modified_other_value["target"] = prefix_sql

        # Delegate to is_contained_by logic
        from .is_contained_by_operator import IsContainedByOperator

        is_contained_by_operator = IsContainedByOperator(self.original_data)
        return is_contained_by_operator.execute_operator(modified_other_value)
