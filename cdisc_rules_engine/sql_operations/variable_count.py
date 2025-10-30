from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableCountOperation(SqlBaseOperation):
    def _execute_operation(self):
        all_tables = self.data_service.pgi.schema.get_tables()
        data_tables = [(name, schema) for name, schema in all_tables if schema.source == "data"]

        target = self.params.target
        domain = self.params.domain.upper()

        if target.upper().startswith(domain):
            wildcard_target = "--" + target[len(domain) :]
        else:
            wildcard_target = target

        count = 0
        for table_name, table_schema in data_tables:
            if wildcard_target.startswith("--"):
                target_variable = table_name.upper() + wildcard_target[2:]
            else:
                target_variable = wildcard_target

            if table_schema.has_column(target_variable):
                count += 1

        query = f"SELECT {count} AS value"
        return SqlOperationResult(query=query, type="constant", subtype="Num")
