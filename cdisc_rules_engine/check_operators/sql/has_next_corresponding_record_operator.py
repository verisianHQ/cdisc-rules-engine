from .base_sql_operator import BaseSqlOperator


class HasNextCorrespondingRecordOperator(BaseSqlOperator):
    """Operator for checking if record has next corresponding record."""

    def execute_operator(self, other_value):
        raise NotImplementedError("has_next_corresponding_record check_operator not implemented")
