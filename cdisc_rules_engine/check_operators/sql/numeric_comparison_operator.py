from .base_sql_operator import BaseSqlOperator


class NumericComparisonOperator(BaseSqlOperator):
    """Operator for numeric comparisons."""

    def __init__(self, data, operator="<"):
        super().__init__(data)
        self.operator = operator

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target"))
        comparator = self.replace_prefix(other_value.get("comparator"))

        try:
            target_column = self._sql(target)
            comparator_column = self._sql(comparator)
        except KeyError:
            target_column = None
            comparator_column = None

        def sql():
            if target_column is not None and comparator_column is not None:
                return f"""NOT ({self._is_empty_sql(target)})
                            AND NOT ({self._is_empty_sql(comparator)})
                            AND CAST({target_column} AS NUMERIC)
                                {self.operator}
                                CAST({comparator_column} AS NUMERIC)
                            """
            else:
                return "FALSE"

        return self._do_check_operator(f"{target_column}{self.operator}{comparator}", sql)
