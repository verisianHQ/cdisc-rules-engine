from .base_sql_operator import BaseSqlOperator


class InconsistentEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking inconsistent enumerated columns."""

    def execute_operator(self, other_value):
        raise NotImplementedError("inconsistent_enumerated_columns check_operator not implemented")
