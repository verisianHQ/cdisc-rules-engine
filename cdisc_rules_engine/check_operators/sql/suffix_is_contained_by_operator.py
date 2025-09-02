from .base_sql_operator import BaseSqlOperator


class SuffixIsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target suffix is contained by the comparator."""

    def execute_operator(self, other_value):
        """
        Checks if target suffix is contained by the comparator.
        """
        target = self.replace_prefix(other_value.get("target"))
        suffix_length = other_value.get("suffix")
        suffix_sql = f"RIGHT({target}, {suffix_length})"

        # Create new other_value with the suffix SQL as target
        modified_other_value = other_value.copy()
        modified_other_value["target"] = suffix_sql

        # Delegate to is_contained_by logic
        from .is_contained_by_operator import IsContainedByOperator

        is_contained_by_operator = IsContainedByOperator(self.original_data)
        return is_contained_by_operator.execute_operator(modified_other_value)
