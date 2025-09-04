from .base_sql_operator import BaseSqlOperator


class ExistsOperator(BaseSqlOperator):
    """Operator for checking column existence."""

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target"))
        result = self._exists(target_column)
        if self.invert:
            result = not result
        suffix = "notexists" if self.invert else "exists"
        return self._do_check_operator(f"""{target_column}_{suffix}""", lambda: "TRUE" if result else "FALSE")
