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
        target = other_value.get("target")
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        case_insensitive = other_value.get("case_insensitive", False)
        regex = other_value.get("regex", None)

        if not isinstance(target, str):
            return self._do_check_operator(lambda: "FALSE")

        target_variable = self.replace_prefix(target)
        matching_columns = self._find_enumerated_columns(target_variable, regex=regex)

        if not matching_columns:
            return self._do_check_operator(lambda: "FALSE")

        comp_sql = self._sql(comparator, lowercase=case_insensitive, value_is_literal=value_is_literal)
        comp_empty_sql = self._is_empty_sql(comparator)

        conditions = []
        for col in matching_columns:
            col_sql = self._column_sql(col, lowercase=case_insensitive)
            col_empty_sql = self._is_empty_sql(col)

            condition = f"(NOT ({col_empty_sql}) AND NOT ({comp_empty_sql}) AND {col_sql} = {comp_sql})"
            conditions.append(condition)

        sql = " OR ".join(conditions)

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

        if regex:
            pattern = rf"{regex}"
        else:
            pattern = rf"^{re.escape(target_variable)}(\d*)$"

        for column_name, _ in all_columns:
            if re.match(pattern, column_name, re.IGNORECASE):
                matching_columns.append(column_name.lower())

        def sort_key(col_name):
            standard_match = re.match(rf"^{re.escape(target_variable)}(\d*)$", col_name, re.IGNORECASE)
            if standard_match:
                suffix = standard_match.group(1)
                return (0, col_name) if suffix == "" else (int(suffix), col_name)
            number_match = re.search(r"(\d+)$", col_name)
            if number_match:
                return (int(number_match.group(1)), col_name)
            return (float("inf"), col_name)

        return sorted(matching_columns, key=sort_key)
