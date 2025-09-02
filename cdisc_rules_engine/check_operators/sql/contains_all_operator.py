from .base_sql_operator import BaseSqlOperator


class ContainsAllOperator(BaseSqlOperator):
    """Operator for checking if value contains all expected elements."""

    def execute_operator(self, other_value):
        """target = self.replace_prefix(other_value.get("target"))
        value_is_literal: bool = other_value.get("value_is_literal", False)
        comparator: list = other_value.get("comparator")
        column_data = self.validation_df[target]
        # We need to check that ALL elements in comparator are contained in the target iterables
        if self.is_column_of_iterables(column_data):
            results = column_data.apply(lambda x: set(comparator).issubset(set(x)) if isinstance(x, (list, set))
              else False)
        else:
            results = False
        return results"""
        raise NotImplementedError("contains_all check_operator not implemented")
