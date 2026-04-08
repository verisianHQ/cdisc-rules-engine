from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class ValidUniiCodeTermPairsOperator(BaseSqlOperator):
    """Validates corresponding UNII code-term pairs simultaneously."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator_column = self.replace_prefix(other_value.get("comparator")).lower()

        if target_column.endswith("CD"):
            code_col = target_column
            term_col = comparator_column
        else:
            term_col = target_column
            code_col = comparator_column

        query = f"""
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {StaticTables.UNII_TABLE_NAME.value}
                    WHERE term_code = CAST({self._column_sql(code_col, alias=False)} AS TEXT)
                      AND term_name = CAST({self._column_sql(term_col, alias=False)} AS TEXT)
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
