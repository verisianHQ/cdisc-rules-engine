from .base_sql_operator import BaseSqlOperator


class EqualsStringPartOperator(BaseSqlOperator):
    """Operator for checking if string part equals comparator."""

    def execute_operator(self, other_value):
        """target: str = self.replace_prefix(other_value.get("target"))
        comparator: Union[str, Any] = (
            self.replace_prefix(other_value.get("comparator"))
            if not other_value.get("value_is_literal", False)
            else other_value.get("comparator")
        )
        comparison_data = self.get_comparator_data(comparator, other_value.get("value_is_literal", False))
        length: int = other_value.get("length")
        part: str = other_value.get("part")  # "prefix" or "suffix"
        return self._check_equality_of_string_part(target, comparison_data, part, length)"""
        raise NotImplementedError("equals_string_part check_operator not implemented")
