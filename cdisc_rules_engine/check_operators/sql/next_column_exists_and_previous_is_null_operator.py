from .base_sql_operator import BaseSqlOperator


class NextColumnExistsAndPreviousIsNullOperator(BaseSqlOperator):
    """Operator for checking if next column exists and previous is null."""

    def execute_operator(self, other_value):
        raise NotImplementedError("next_column_exists_and_previous_is_null check_operator not implemented")
