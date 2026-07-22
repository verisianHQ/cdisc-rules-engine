from cdisc_rules_engine.enums.static_tables import StaticTables

from .base_sql_operator import BaseSqlOperator


class IsExtensibleCodelistCodeOperator(BaseSqlOperator):
    """Returns TRUE if the value of a variable is in the extensible codelist."""

    def execute_operator(self, other_value):
        target = other_value.get("target")

        if isinstance(target, str) and self._exists(target):
            target_column = self.replace_prefix(target).lower()
            target_sql = self._column_sql(target_column, alias=False, null_return=True)
        elif isinstance(target, str) and target in self.operation_variables:
            target_op = self.operation_variables[target]
            if target_op.type == "constant":
                target_sql = self._process_constant_operation_variable(target)
            else:
                raise ValueError(f"Unsupported operation variable type: {target_op.type}. Must be 'constant'.")
        else:
            raise ValueError(
                f"Target '{target}' not found in dataset or operation variables. "
                "Must be a variable in the dataset or an operation variable of type 'constant'."
            )

        def sql():
            return f"""
                CASE
                    WHEN {target_sql} IN (
                        SELECT codelist_code
                        FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}
                        WHERE extensible = 'Yes'
                    ) THEN TRUE
                    ELSE FALSE
                END
            """

        return self._do_check_operator(sql)
