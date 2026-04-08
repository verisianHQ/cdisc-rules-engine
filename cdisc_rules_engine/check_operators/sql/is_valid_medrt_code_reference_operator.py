from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class ValidMedRTCodeReferenceOperator(BaseSqlOperator):
    """Validates terminology codes against the reference Med-RT dictionary."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        query = f"""
            CASE
                WHEN CAST({self._column_sql(target_column, alias=False)} AS TEXT) IN (
                    SELECT term_code
                    FROM {StaticTables.MEDRT_TABLE_NAME.value}
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
