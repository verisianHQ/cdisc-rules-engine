from .base_sql_operator import BaseSqlOperator


class SharesNoElementsWithOperator(BaseSqlOperator):
    """Operator for checking if values share no elements."""

    def execute_operator(self, other_value):
        raise NotImplementedError("shares_no_elements_with check_operator not implemented")
