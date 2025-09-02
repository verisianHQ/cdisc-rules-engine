from .base_sql_operator import BaseSqlOperator


class GetStringPartSeriesOperator(BaseSqlOperator):
    """Operator for getting string part series from data."""

    def execute_operator(self, other_value):
        raise NotImplementedError("get_string_part_series check_operator not implemented")
