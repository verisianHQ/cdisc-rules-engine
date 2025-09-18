from .suffix_matches_regex_operator import SuffixMatchesRegexOperator


class NotSuffixMatchesRegexOperator(SuffixMatchesRegexOperator):
    """Operator for inverted suffix regex pattern matching."""

    def execute_operator(self, other_value):
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        suffix = other_value.get("suffix")

        def sql():
            length_expr = f"LENGTH({target_column}::text) - {suffix} + 1"
            substring_expr = f"SUBSTRING({target_column}::text, {length_expr})"
            return f"""CASE WHEN
                            NOT ({self._is_empty_sql(target)})
                            AND NOT ({substring_expr} ~ '{comparator}')
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(f"{target_column}_not_suffix_matches_regex", sql)
