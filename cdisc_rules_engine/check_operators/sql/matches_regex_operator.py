from .base_sql_operator import BaseSqlOperator


class MatchesRegexOperator(BaseSqlOperator):
    """Operator for regex pattern matching."""

    def __init__(self, data, invert=False, case_insensitive=False):
        super().__init__(data)
        self.invert = invert
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        regex_op = "~*" if self.case_insensitive else "~"

        def sql():
            return f"""CASE WHEN
                            NOT ({self._is_empty_sql(target)})
                            AND {'NOT' if self.invert else ''} {target_column}::text {regex_op} '{comparator}'
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(sql)
