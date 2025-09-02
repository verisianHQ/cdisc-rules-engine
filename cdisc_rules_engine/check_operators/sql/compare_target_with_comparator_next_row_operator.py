from .base_sql_operator import BaseSqlOperator


class CompareTargetWithComparatorNextRowOperator(BaseSqlOperator):
    """Operator for comparing target with comparator in next row."""

    def execute_operator(self, other_value):
        raise NotImplementedError("compare_target_with_comparator_next_row check_operator not implemented")
