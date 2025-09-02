from .base_sql_operator import BaseSqlOperator


class ValueHasMultipleReferencesOperator(BaseSqlOperator):
    """Operator for checking if value has multiple references."""

    def execute_operator(self, other_value):
        raise NotImplementedError("value_has_multiple_references check_operator not implemented")
