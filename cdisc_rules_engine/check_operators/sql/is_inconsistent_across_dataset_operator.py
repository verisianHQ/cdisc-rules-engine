from .base_sql_operator import BaseSqlOperator


class IsInconsistentAcrossDatasetOperator(BaseSqlOperator):
    """Operator for checking if values are inconsistent across dataset."""

    def execute_operator(self, other_value):
        # This operator is not yet implemented in the original SQL version
        # The original version has this as NotImplementedError with commented code
        raise NotImplementedError("is_inconsistent_across_dataset check_operator not implemented")
