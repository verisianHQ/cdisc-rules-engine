from .base_sql_operator import BaseSqlOperator


class PrefixSuffixMatchesRegexOperator(BaseSqlOperator):
    """Operator for regex pattern matching on string prefixes or suffixes."""

    def execute_operator(self, other_value):
        if "prefix" in other_value:
            mode = "prefix"
        elif "suffix" in other_value:
            mode = "suffix"
        else:
            raise ValueError("Missing 'suffix' or 'prefix' key in operator parameters.")
        target = self.replace_prefix(other_value.get("target")).lower()
        target_column = self._column_sql(target)
        comparator = other_value.get("comparator")
        length = other_value.get(mode)
        return self._handle_regex_comparator(target_column, comparator, length, mode)

    def _handle_regex_comparator(self, target_column, comparator, length, mode):
        """Handle regex comparison for prefix or suffix."""
        cache_key = f"{target_column}_{mode}_matches_regex_{str(comparator).replace(' ', '_')}_{length}"

        def sql():
            target_sql = self._column_sql(target_column)
            if mode == "prefix":
                substring_sql = f"SUBSTRING({target_sql}::text, 1, {length})"
            else:
                substring_sql = f"SUBSTRING({target_sql}::text, LENGTH({target_sql}::text) - {length} + 1)"
            regex_condition = f"{substring_sql} ~ '{comparator}'"
            return f"""CASE WHEN
                            {target_sql} IS NOT NULL
                            AND {regex_condition}
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(cache_key, sql)
