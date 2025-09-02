from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def execute_operator(self, other_value):
        column = self.replace_prefix(other_value.get("target"))

        def sql():
            return self._is_empty_sql(column)

        return self._do_check_operator(f"{column}_empty", sql)

    def _is_empty_sql(self, col: str) -> str:
        """
        Generates a SQL query to check if a column is empty.
        """
        column = self.sql_data_service.pgi.schema.get_column(self.table_id, col)
        if not column:
            raise ValueError(f"Column {col} does not exist in the table {self.table_id}.")

        match column.type:
            case "Char":
                return f"({column.hash} IS NULL OR {column.hash} = '')"
            case "Bool":
                return f"({column.hash} IS NULL)"
            case "Num":
                return f"({column.hash} IS NULL)"
            case _:
                raise ValueError(f"Unsupported column type: {column.type} for column {col}.")
