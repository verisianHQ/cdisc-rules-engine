from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def execute_operator(self, other_value):
        column = self.replace_prefix(other_value.get("target"))

        def sql():
            return self._is_empty_sql(column)

        return self._do_check_operator(sql)

    def has_missing_required_columns(self, other_value) -> bool:
        target = self.replace_prefix(other_value.get("target"))
        if not isinstance(target, str) or target == "":
            return False
        if target in self.operation_variables:
            return False
        return self.sql_data_service.pgi.schema.get_column(self.table_id, target) is None
