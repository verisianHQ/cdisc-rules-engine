from .base_sql_operator import BaseSqlOperator


class SharesAtLeastOneElementWithOperator(BaseSqlOperator):
    """Operator for checking if values share at least one element."""

    def execute_operator(self, other_value):
        raise NotImplementedError("shares_at_least_one_element_with check_operator not implemented")
