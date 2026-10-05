from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableIsNullOperation(SqlBaseOperation):
    """
    Whether the target variable is null across the whole dataset.
    The target is looked up in the domain first, then in the rule table (e.g. define or
    library metadata columns).
    True when every record is null.
    For Char variables '' and values containing only whitespace also count as null.
    An empty dataset is TRUE.
    """

    def _execute_operation(self):
        schema = self.data_service.pgi.schema
        column = schema.get_column(self.params.domain, self.params.target)
        if column is not None:
            return self._constant(self._is_null_sql(self.params.domain, column))

        rule_table_column = schema.get_column(self.params.table, self.params.target) if self.params.table else None
        if rule_table_column is None:
            raise ColumnNotFoundError(column_name=self.params.target, table_id=self.params.domain)
        return self._constant(self._is_null_sql(self.params.table, rule_table_column))

    def _is_null_sql(self, table: str, column: SqlColumnSchema) -> str:
        table_id = self.data_service.pgi.schema.get_table_hash(table)
        if column.type == "Char":
            is_empty = f"({column.hash} IS NULL OR REGEXP_REPLACE({column.hash}, '\\s', '', 'g') = '')"
        else:
            is_empty = f"({column.hash} IS NULL)"
        return f"(NOT EXISTS (SELECT 1 FROM {table_id} WHERE NOT {is_empty}))"

    @staticmethod
    def _constant(value_sql: str) -> SqlOperationResult:
        return SqlOperationResult(query=f"SELECT {value_sql} AS value", type="constant", subtype="Bool")
