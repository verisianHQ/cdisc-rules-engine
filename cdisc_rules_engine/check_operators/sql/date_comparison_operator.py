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
                "year": "YEAR",
                "month": "MONTH",
                "day": "DAY",
                "hour": "HOUR",
                "minute": "MINUTE",
                "second": "SECOND",
                "microsecond": "MICROSECONDS",
            }
            pg_component = component_map.get(date_component, "EPOCH")

            if isinstance(target, str) and self._exists(target.lower()):
                target = target.lower()
                target_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, target)
                wrapped_target = target_date_column.hash
            else:
                wrapped_target = f"CAST ({self._sql(target)} AS TIMESTAMP)"

            if isinstance(comparator, str) and not value_is_literal and self._exists(comparator.lower()):
                comparator = comparator.lower()
                comparator_date_column = self.sql_data_service.pgi.generate_date_column(self.table_id, comparator)
                wrapped_comparator = comparator_date_column.hash
            else:
                wrapped_comparator = f"CAST ({self._sql(comparator, value_is_literal=value_is_literal)} AS TIMESTAMP)"

            wrapped_target = f"EXTRACT({pg_component} FROM {wrapped_target})"
            wrapped_comparator = f"EXTRACT({pg_component} FROM {wrapped_comparator})"

        def sql():
            trunc_target, trunc_comparator = self._build_truncated_comparison_elements(
                wrapped_target, wrapped_comparator
            )
            comparison_sql = f"{trunc_target} {self.operator} {trunc_comparator}"

            sql = f"""CASE WHEN
                NOT ({self._is_empty_sql(target)})
                AND NOT ({self._is_empty_sql(comparator)})
                AND {comparison_sql}
                THEN true
                ELSE false
                END"""

            return sql

        return self._do_check_operator(sql)
