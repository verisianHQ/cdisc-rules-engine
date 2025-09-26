from .base_sql_operator import BaseSqlOperator


class IsOrderedSetOperator(BaseSqlOperator):
    """
    True if the dataset rows are in ascending order of the values within `name`, grouped by the values within `value`.
    """

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        name = self.replace_prefix(other_value.get("target"))
        name_column = self._column_sql(name)
        value = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        if not value_is_literal:
            value = self.replace_prefix(value)
        value_column = self._column_sql(value) if not value_is_literal else self._constant_sql(value)
        if self.invert:
            operator_name = f"{name}_is_not_ordered_set_within_{value}"
        else:
            operator_name = f"{name}_is_ordered_set_within_{value}"

        def sql():
            table_ref = self._table_sql()
            dataset_is_ordered = f"""
            NOT EXISTS (
                SELECT 1
                FROM {table_ref} t1
                INNER JOIN {table_ref} t2 ON t1.{value_column} = t2.{value_column}
                WHERE t1.{name_column} IS NOT NULL
                  AND t2.{name_column} IS NOT NULL
                  AND t1.ctid < t2.ctid
                  AND t1.{name_column} > t2.{name_column}
            )
            """

            if self.invert:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    WHEN {name_column} IS NULL THEN TRUE
                    WHEN {value_column} IS NULL THEN FALSE
                    ELSE NOT ({dataset_is_ordered})
                END
                """
            else:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    WHEN {name_column} IS NULL THEN FALSE
                    WHEN {value_column} IS NULL THEN FALSE
                    ELSE {dataset_is_ordered}
                END
                """

        return self._do_check_operator(operator_name, sql)
