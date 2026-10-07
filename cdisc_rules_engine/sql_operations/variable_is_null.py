from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundError
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableIsNullOperation(SqlBaseOperation):
    """
    Whether the target variable is null across the whole dataset.
    True when every record is null or when the variable is not in the dataset.
    For Char variables '' and values containing only whitespace also count as null.
    An empty dataset is TRUE.
    The variable is looked up in the operation's domain, or in the rule type's table
    (e.g. define or library metadata columns) when use_rule_type_table is set.
    """

    def _execute_operation(self):
        table = self.params.table if self.params.use_rule_type_table else self.params.domain
        schema = self.data_service.pgi.schema
        if not table or not schema.table_exists(table):
            raise DomainNotFoundError(f"Operation variable_is_null requires Domain {table} but Domain not found")

        column = schema.get_column(table, self.params.target)
        if column is None:
            return self._constant("TRUE")
        return self._constant(self._is_null_sql(table, column))

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
