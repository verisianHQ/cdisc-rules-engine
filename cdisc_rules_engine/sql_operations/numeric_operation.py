from cdisc_rules_engine.sql_operations.aggregate_operation import AggregateSource, SqlAggregateOperation


class SqlNumericOperation(SqlAggregateOperation):

    def _dataset_value(self, table: str):
        # Special case for counting size of whole dataset
        if self.params.target is None:
            return "*", "Num"
        return self.data_service.pgi.schema.get_column_hash(table, self.params.target), "Num"

    def _aggregate_sql(self, value_sql: str, aggregate_filter: str = "") -> str:
        return f"{self.function}({value_sql}){aggregate_filter}"

    def _result_subtype(self, source: AggregateSource) -> str:
        return "Num"
