from .base_sql_operator import BaseSqlOperator


class DateComparisonOperator(BaseSqlOperator):
    """Operator for date comparisons."""

    def __init__(self, data, operator="="):
        super().__init__(data)
        self.operator = operator

    def _sanitise_iso_sql(self, expr: str) -> str:
        """Returns the string if it conforms to strict ISO-8601 padding, otherwise NULL."""
        iso_pattern = r"^\d{4}(-\d{2}(-\d{2}([T ]\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:?\d{2})?)?)?)?$"
        return f"CASE WHEN {expr}::text ~ '{iso_pattern}' THEN {expr}::text ELSE NULL END"

    def _build_truncated_comparison_elements(self, wrapped_target: str, wrapped_comparator: str) -> str:
        """
        Truncates the longer date/datetime expression to the character length
        of the shorter one before applying the operator.
        """
        valid_target = self._sanitise_iso_sql(wrapped_target)
        valid_comparator = self._sanitise_iso_sql(wrapped_comparator)

        min_len = f"LEAST(LENGTH({valid_target}::text), LENGTH({valid_comparator}::text))"
        trunc_target = f"LEFT({valid_target}::text, {min_len})"
        trunc_comparator = f"LEFT({valid_comparator}::text, {min_len})"

        return f"{trunc_target}", f"{trunc_comparator}"

    def execute_operator(self, other_value):
        """
        Performs date comparison operations in PostgreSQL.
        Handles date component extraction and comparison.
        """
        target = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        date_component = other_value.get("date_component")

        if isinstance(comparator, str) and not value_is_literal:
            comparator = self.replace_prefix(comparator)

        wrapped_target = f"CAST ({self._sql(target)} AS TEXT)"
        wrapped_comparator = f"CAST ({self._sql(comparator)} AS TEXT)"

        if date_component:
            component_map = {
                "year": ("YEAR", 4, 0),
                "month": ("MONTH", 7, 5),
                "day": ("DAY", 10, 8),
                "hour": ("HOUR", 13, 11),
                "minute": ("MINUTE", 16, 14),
                "second": ("SECOND", 19, 17),
                "microsecond": ("MICROSECONDS", 26, 20),
            }
            pg_component, trunc_length, start_pos = component_map.get(date_component, ("EPOCH", 0, 0))

            wrapped_target = f"CASE WHEN LENGTH({wrapped_target}) >= {trunc_length} THEN SUBSTRING({wrapped_target}, {start_pos}, 1+{trunc_length}-{start_pos}) ELSE NULL END"  # noqa
            wrapped_comparator = f"CASE WHEN LENGTH({wrapped_comparator}) >= {trunc_length} THEN SUBSTRING({wrapped_comparator}, {start_pos}, 1+{trunc_length}-{start_pos}) ELSE NULL END"  # noqa

        def sql():
            if not date_component:
                trunc_target, trunc_comparator = self._build_truncated_comparison_elements(
                    wrapped_target, wrapped_comparator
                )
                comparison_sql = f"{trunc_target} {self.operator} {trunc_comparator}"
            else:
                comparison_sql = f"{wrapped_target} {self.operator} {wrapped_comparator}"

            sql = f"""CASE WHEN
                NOT ({self._is_empty_sql(target)})
                AND NOT ({self._is_empty_sql(comparator)})
                AND {comparison_sql}
                THEN true
                ELSE false
                END"""

            print(sql)

            return sql

        return self._do_check_operator(sql)
