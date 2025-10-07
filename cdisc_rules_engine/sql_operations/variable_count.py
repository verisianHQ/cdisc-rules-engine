from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableCountOperation(SqlBaseOperation):
    def _execute_operation(self):
        all_tables = self.data_service.pgi.schema.get_tables()
        data_tables = [name for name, schema in all_tables if schema.source == "data"]

        count = 0

        for domain_name in data_tables:
            variable_name = self.params.target.replace("--", domain_name)

            if self.data_service.pgi.schema.column_exists(domain_name, variable_name):
                count += 1

        query = f"SELECT {count} AS value"
        return SqlOperationResult(query=query, type="constant", subtype="Num")
