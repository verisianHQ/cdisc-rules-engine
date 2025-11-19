from .base_sql_operator import BaseSqlOperator


class MatchesRegexOperator(BaseSqlOperator):
    """Operator for regex pattern matching."""

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        try:
            target_column = self._column_sql(target)
        except KeyError:
            target_column = None
        comparator = other_value.get("comparator")

        def sql():
            if target_column:
                return f"""CASE WHEN
                                NOT ({self._is_empty_sql(target)})
                                AND {'NOT' if self.invert else ''} {target_column}::text ~ '{comparator}'
                            THEN true
                            ELSE false
                            END"""
            else:
                return "FALSE"

        return self._do_check_operator(f"{target_column}_{self.invert}_matches_{comparator}", sql)
