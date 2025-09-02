from .is_contained_by_operator import IsContainedByOperator


class IsContainedByCaseInsensitiveOperator(IsContainedByOperator):
    """Operator for case-insensitive containment checks."""

    def execute_operator(self, other_value):
        comparator = other_value["comparator"]
        if isinstance(comparator, list):
            comparator = [str(v).lower() for v in comparator]
        elif isinstance(comparator, str):
            comparator = comparator.lower()

        return super().execute_operator(
            {
                "target": f"LOWER({self.replace_prefix(other_value['target']).lower()})",
                "comparator": comparator,
                "value_is_literal": other_value.get("value_is_literal", False),
            }
        )
