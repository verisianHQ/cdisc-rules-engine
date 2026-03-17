from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlValidWhodrugReferencesOperation(SqlBaseOperation):
    def execute(self) -> SqlOperationResult:
        query = f"""
            SELECT DISTINCT val
            FROM (
                SELECT level_1 AS val FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                UNION ALL
                SELECT level_2 FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                UNION ALL
                SELECT level_3 FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                UNION ALL
                SELECT level_4 FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                UNION ALL
                SELECT drug_name FROM {StaticTables.WHODRUG_TABLE_NAME.value}
            ) AS value
            WHERE val IS NOT NULL;
        """
        return SqlOperationResult(query=query, type="collection", subtype="Char")
