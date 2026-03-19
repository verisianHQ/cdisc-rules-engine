from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class IsValidWhodrugReferenceOperator(BaseSqlOperator):
    """Checks if a value in a target column is a valid WHODrug reference."""

    def execute_operator(self, other_value):
        target_column = other_value.get("target").lower()
        query = f"""
            SELECT
                id,
                CASE
                    WHEN {self._column_sql(target_column, alias=False)} IN (
                        SELECT atc_code FROM {self._table_sql(StaticTables.WHODRUG_TABLE_NAME.value)}
                    ) THEN TRUE
                    ELSE FALSE
                END AS is_valid_whodrug_reference
            FROM {self._table_sql()}
        """
        return self._do_check_operator(lambda: query)
