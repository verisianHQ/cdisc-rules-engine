from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError

from .base_sql_operator import BaseSqlOperator


class HasDifferentValuesOperator(BaseSqlOperator):
    """Operator for checking if a column has different values."""

    def _execute_operator_impl(self, other_value):
        target_column = other_value.get("target").lower()
        if not self._exists(target_column):
            raise ColumnNotFoundError(target_column, self.table_id)
        operation_name = f"{target_column}_has_different_values"

        return self._do_check_operator(
            operation_name, lambda: f"(SELECT COUNT(DISTINCT {target_column}) FROM {self._table_sql()}) > 1"
        )

    def get_result_for_missing_columns(self):
        return "FALSE"
