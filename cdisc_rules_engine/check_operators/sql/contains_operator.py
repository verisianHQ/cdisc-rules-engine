from .base_sql_operator import BaseSqlOperator
from .is_contained_by_operator import IsContainedByOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the comparator value(s) are contained within the target column values.
        This is implemented by calling is_contained_by with target and comparator swapped.
        """
        if other_value.get("value_is_literal", False):
            return IsContainedByOperator(self.original_data, case_insensitive=self.case_insensitive).execute_operator(
                other_value
            )
        else:
            swapped = {
                "target": other_value["comparator"],
                "comparator": other_value["target"],
                "value_is_literal": other_value.get("value_is_literal", False),
            }
            return IsContainedByOperator(self.original_data, case_insensitive=self.case_insensitive).execute_operator(
                swapped
            )
