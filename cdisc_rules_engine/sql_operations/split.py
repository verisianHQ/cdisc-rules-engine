from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlSplitOperation(SqlBaseOperation):
    """
    Splits a delimited string (from a constant, collection, or column) into a collection of values.
    Uses PostgreSQL UNNEST(STRING_TO_ARRAY()) to generate a multi-row table result.
    """

    def _execute_operation(self):
        target = self.params.target
        delimiter = self.params.delimiter or ","

        if target in self.params.previous_operations:
            op_result = self.params.previous_operations[target]

            if op_result.type == "constant":
                query = f"""
                    SELECT TRIM(UNNEST(STRING_TO_ARRAY(({op_result.query})::text, '{delimiter}'))) AS value
                    WHERE ({op_result.query}) IS NOT NULL AND ({op_result.query})::text != ''
                """
            elif op_result.type == "collection":
                query = f"""
                    SELECT TRIM(UNNEST(STRING_TO_ARRAY(op_vals.value::text, '{delimiter}'))) AS value
                    FROM ({op_result.query}) AS op_vals
                    WHERE op_vals.value IS NOT NULL AND op_vals.value::text != ''
                """
            else:
                raise ValueError(f"Cannot perform split operation on unsupported operation type: {op_result.type}")

        elif self.data_service.pgi.schema.column_exists(self.params.domain, target):
            dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
            column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, target)

            where_clause = self.construct_where_clause()

            query = f"""
                SELECT DISTINCT TRIM(UNNEST(STRING_TO_ARRAY({column_id}::text, '{delimiter}'))) AS value
                FROM {dataset_id}
                {where_clause}
                WHERE {column_id} IS NOT NULL AND {column_id}::text != ''
            """

        else:
            raise ValueError(f"Target '{target}' is not a valid operation variable or dataset column.")

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=None)
