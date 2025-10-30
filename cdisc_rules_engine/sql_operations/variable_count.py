from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableCountOperation(SqlBaseOperation):
    def _execute_operation(self):
        all_tables = self.data_service.pgi.schema.get_tables()

        data_tables = [(name, schema) for name, schema in all_tables if schema.source == "data"]

        count = 0
        for _, table_schema in data_tables:
            if table_schema.has_column(self.params.target):
                count += 1

        query = f"SELECT {count} AS value"

        return SqlOperationResult(query=query, type="constant", subtype="Num")
