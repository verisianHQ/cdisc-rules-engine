from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableExists(SqlBaseOperation):
    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)

        query = f"""SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS value
                    FROM information_schema.columns
                    WHERE table_name = '{dataset_id}' AND column_name = '{column_id}'"""

        return SqlOperationResult(query=query, type="constant", subtype="Bool")
