from .base_sql_operator import BaseSqlOperator


class PrefixMatchesRegexOperator(BaseSqlOperator):
    """Operator for prefix regex pattern matching."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        prefix = other_value.get("prefix")

        def sql():
            substring_expr = f"SUBSTRING({target_column}::text, 1, {prefix})"
            return f"""CASE WHEN
                            NOT ({self._is_empty_sql(target)})
                            AND {substring_expr} ~ '{comparator}'
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(f"{target_column}_prefix_matches_regex", sql)
