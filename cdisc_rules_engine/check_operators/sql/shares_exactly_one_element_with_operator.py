from .base_sql_operator import BaseSqlOperator


class SharesExactlyOneElementWithOperator(BaseSqlOperator):
    """Operator for checking if values share exactly one element."""

    def execute_operator(self, other_value):
        raise NotImplementedError("shares_exactly_one_element_with check_operator not implemented")
