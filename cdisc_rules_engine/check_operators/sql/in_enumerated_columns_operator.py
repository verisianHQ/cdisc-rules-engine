import re
from .base_sql_operator import BaseSqlOperator


class InEnumeratedColumnsOperator(BaseSqlOperator):
    """Operator for checking value presence in enumerated columns."""

    def execute_operator(self, other_value):
        """
        Check for value presence in enumerated columns of a DataFrame.

        Starting with the smallest/largest enumeration of the given variable,
        check if the comparator column matches the target variable.
        Repeat for all variables belonging to the enumeration.
        Note that the initial variable will not have an index (VARIABLE) and
        the next enumerated variable has index 1 (VARIABLE1).
        """
        target_variable = self.replace_prefix(other_value.get("target"))
        comparator = other_value.get("comparator")
        case_insensitive = other_value.get("case_insensitive", False)
        regex = other_value.get("regex", None)

        matching_columns = self._find_enumerated_columns(target_variable, regex=regex)

        if not matching_columns:
            return self._do_check_operator(lambda: "FALSE")

        sql = " OR ".join(
            (
                f"LOWER({self._column_sql(col, alias=False)}) = LOWER('{comparator}')"
                if case_insensitive
                else f"{self._column_sql(col, alias=False)} = '{comparator}'"
            )
            for col in matching_columns
        )

        return self._do_check_operator(lambda: f"CASE WHEN {sql} THEN TRUE ELSE FALSE END")

    def _find_enumerated_columns(self, target_variable: str, regex=None) -> list:
        """
        Find all columns that match the enumeration pattern for the target variable.
        Returns them sorted in enumeration order (base variable first, then numbered).
        """
        table_schema = self.sql_data_service.pgi.schema.get_table(self.table_id)
        if not table_schema:
            return []

        all_columns = table_schema.get_columns()
        matching_columns = []

        # Pattern to match VARIABLE, VARIABLE1, VARIABLE2, etc.
        if regex:
            pattern = rf"{regex}"
        else:
            pattern = rf"^{re.escape(target_variable)}(\d*)$"

        for column_name, column_schema in all_columns:
            if re.match(pattern, column_name, re.IGNORECASE):
                matching_columns.append(column_name.lower())

        def sort_key(col_name):
            match = re.match(rf"^{re.escape(target_variable)}(\d*)$", col_name, re.IGNORECASE)
            if match:
                suffix = match.group(1)
                if suffix == "":
                    return (0, col_name)
                else:
                    return (int(suffix), col_name)
            return (float("inf"), col_name)

        return sorted(matching_columns, key=sort_key)
