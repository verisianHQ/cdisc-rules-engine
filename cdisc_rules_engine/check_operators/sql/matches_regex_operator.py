from .base_sql_operator import BaseSqlOperator


class MatchesRegexOperator(BaseSqlOperator):
    """Operator for regex pattern matching."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")

        def sql():
            return f"""CASE WHEN
                            {target_column} IS NOT NULL 
                            AND {target_column}::text ~ '{comparator}'
                        THEN true
                        ELSE false
                        END"""
        return self._do_check_operator(f"{target_column}_matches_regex", sql)
