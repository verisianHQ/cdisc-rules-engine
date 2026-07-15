from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlNumericOperation(SqlBaseOperation):

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, function: str):
        super().__init__(params, data_service)
        self.function = function

    def _execute_operation(self):
        table = self.params.table if self.params.use_rule_type_table else self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(table)

        # Special case for counting size of whole dataset
        if self.params.target is None:
            column_id = "*"
            case_column_id = "1"
        else:
            column_id = self.data_service.pgi.schema.get_column_hash(table, self.params.target)
            case_column_id = column_id

        where_clause = self.construct_where_clause()

        if not self.params.grouping:
            query = f"SELECT {self.function}({column_id}) AS value FROM {dataset_id} {where_clause}"
            return SqlOperationResult(query=query, type="constant", subtype="Num")
        else:
            grouping_columns = [self.data_service.pgi.schema.get_column(table, group) for group in self.params.grouping]

            partition_by = ", ".join([col.hash for col in grouping_columns])
            id_col = self.data_service.pgi.schema.get_column_hash(table, "id")
            if where_clause.strip():
                filter_condition = where_clause.replace("WHERE", "").strip()
                case_expr = f"CASE WHEN {filter_condition} THEN {case_column_id} ELSE NULL END"
            else:
                case_expr = column_id

            query = f"""
                SELECT
                    {id_col} as id,
                    {self.function}({case_expr}) OVER (PARTITION BY {partition_by}) AS value
                FROM {dataset_id}
            """
            return SqlOperationResult(query=query, type="window", subtype="Num")
