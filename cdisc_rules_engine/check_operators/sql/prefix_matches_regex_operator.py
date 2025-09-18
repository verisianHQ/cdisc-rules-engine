from .base_sql_operator import BaseSqlOperator


class PrefixMatchesRegexOperator(BaseSqlOperator):
    """Operator for prefix regex pattern matching."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        prefix = other_value.get("prefix")

        def sql():
            return f"""CASE WHEN
                            {target_column} IS NOT NULL
                            AND SUBSTRING({target_column}::text, 1, {prefix}) ~ '{comparator}'
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(f"{target_column}_prefix_matches_regex", sql)
