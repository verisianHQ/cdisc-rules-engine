from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableIsNullOperation(SqlBaseOperation):
    """
    Whether the target variable is null across the whole dataset.
    TRUE when the variable is not in the dataset or when every record is null (or '' for Char).
    """

    def _execute_operation(self):
        column = self.data_service.pgi.schema.get_column(self.params.domain, self.params.target)
        if column is None:
            return SqlOperationResult(query="SELECT TRUE AS value", type="constant", subtype="Bool")

        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        if column.type == "Char":
            is_empty = f"({column.hash} IS NULL OR {column.hash} = '')"
        else:
            is_empty = f"({column.hash} IS NULL)"

        query = f"SELECT NOT EXISTS (SELECT 1 FROM {dataset_id} WHERE NOT {is_empty}) AS value"
        return SqlOperationResult(query=query, type="constant", subtype="Bool")
