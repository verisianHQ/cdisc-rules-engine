from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError

from .base_sql_operator import BaseSqlOperator


class IsNullOperator(BaseSqlOperator):
    """Operator for checking if a value is null."""

    def execute_operator(self, other_value):
        original_target = self.replace_prefix(other_value.get("target"))
        target = self._sql(original_target)

        column = self.sql_data_service.pgi.schema.get_column(self.table_id, original_target)
        if not column:
            raise ColumnNotFoundError(original_target, self.table_id)

        query = f"({target} IS NULL OR {target} = '')" if column.type == "Char" else f"{target} IS NULL"

        return self._do_check_operator(lambda: query)
