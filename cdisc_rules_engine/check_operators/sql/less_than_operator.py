from .base_sql_operator import BaseSqlOperator


class LessThanOperator(BaseSqlOperator):
    """Operator for numeric less-than comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, "<")

    def _numeric_comparison(self, other_value: dict, operator: str):
        target_column = self.replace_prefix(other_value.get("target"))
        comparator = (
            other_value.get("comparator").lower()
            if isinstance(other_value.get("comparator"), str)
            else other_value.get("comparator")
        )

        def sql():
            return f"""CASE WHEN
                            CAST({target_column} AS NUMERIC)
                                {operator}
                            CAST({comparator} AS NUMERIC) THEN true
                        ELSE false
                        END
                        """

        return self._do_check_operator(f"{target_column}{operator}{comparator}", sql)
