from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDistinctOperation(SqlBaseOperation):
    def _execute_operation(self):
        table = self.params.table if self.params.use_rule_type_table else self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(table)
        if not dataset_id:
            query = self._empty_result_set_query()
            return SqlOperationResult(query=query, type="collection", subtype="Char", params={})

        target_column = self.data_service.pgi.schema.get_column(table, self.params.target)
        if target_column is None:
            raise ValueError(f"Target column '{self.params.target}' not found in domain/table '{table}'.")

        where_conditions = []
        params = {}

        if self.params.grouping:
            g_conditions, params = self._build_grouping_conditions(table)
            where_conditions.extend(g_conditions)

        if self.params.filter:
            where_conditions.extend(self._build_filter_conditions(table))

        query = f"SELECT DISTINCT {target_column.hash} AS value FROM {dataset_id}"
        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"

        return SqlOperationResult(
            query=query, type="collection", subtype=target_column.type, params=params if params else None
        )

    def _build_grouping_conditions(self, table):
        where_conditions = []
        params = {}
        for group in self.params.grouping:
            col = self.data_service.pgi.schema.get_column(table, group)
            if col is None:
                raise ValueError(f"Grouping column '{group}' not found in domain/table '{table}'.")
            i = len(params)
            param_name = f"${i + 1}"
            where_conditions.append(f"{col.hash} = {param_name}")
            params[param_name] = col.name
        return where_conditions, params

    def _build_filter_conditions(self, table):
        where_conditions = []
        for k, v in self.params.filter.items():
            filter_col = self.data_service.pgi.schema.get_column_hash(table, k)
            if not filter_col:
                raise ValueError(f"Filter column '{k}' not found in domain/table '{table}'")
            where_conditions.append(f"{filter_col} = '{v}'")
        return where_conditions
