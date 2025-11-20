from .base_sql_operator import BaseSqlOperator
from .equal_to_operator import EqualToOperator


class IsContainedByOperator(BaseSqlOperator):
    """Operator for checking if target values are contained by comparator collections."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the target column values are contained within the comparator.

        Returns True if target value exists in the comparator collection/column and is not null/empty.
        Returns False if target value is null, empty, or not found in comparator.

        Handles three types of comparators:
        1. List of literal values - check if target is in the list
        2. Column name (when value_is_literal=False) - checks if target value exists anywhere in the comparator column
        3. Single literal value - checks direct equality with the target
        """
        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        prefix = other_value.get("prefix", None)
        suffix = other_value.get("suffix", None)

        try:
            column = self._column_sql(target_column, lowercase=self.case_insensitive, prefix=prefix, suffix=suffix)
        except KeyError:
            column = None

        cache_key = (
            f"{target_column}_contained_by_{comparator}_{value_is_literal}_{self.case_insensitive}_{prefix}_{suffix}"
        )

        if self._is_collection_comparator(comparator):
            return self._handle_collection_comparator(target_column, column, comparator, cache_key)
        elif self._is_column_comparator(comparator, value_is_literal):
            return self._handle_column_comparator(target_column, column, comparator, cache_key)
        else:
            return self._handle_literal_comparator(other_value)

    def _is_collection_comparator(self, comparator):
        """Check if comparator is a list or operation variable collection."""
        return isinstance(comparator, list) or (isinstance(comparator, str) and comparator in self.operation_variables)

    def _is_column_comparator(self, comparator, value_is_literal):
        """Check if comparator is a column name."""
        return isinstance(comparator, str) and not value_is_literal and self._exists(comparator)

    def _handle_collection_comparator(self, target_column, column, comparator, cache_key):
        """Handle list or collection operation variable comparators."""

        def sql():
            if column is None:
                return "FALSE"
            return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN {self._collection_sql(comparator, lowercase=self.case_insensitive)}"""

        return self._do_check_operator(cache_key, sql)

    def _handle_column_comparator(self, target_column, column, comparator, cache_key):
        """Handle column name comparators."""

        def sql():
            if column is None:
                return "FALSE"
            try:
                comparator_col = self._column_sql(comparator, lowercase=self.case_insensitive, alias=False)
                return f"""NOT ({self._is_empty_sql(target_column)})
                          AND {column} IN (
                              SELECT DISTINCT {comparator_col}
                              FROM {self._table_sql()}
                              WHERE NOT ({self._is_empty_sql(comparator, alias=False)})
                          )"""
            except KeyError:
                return "FALSE"

        return self._do_check_operator(cache_key, sql)

    def _handle_literal_comparator(self, other_value):
        """Handle literal value comparators using EqualToOperator."""
        return EqualToOperator(self.original_data, case_insensitive=self.case_insensitive).execute_operator(
            {"target": other_value.get("target"), "comparator": other_value.get("comparator"), "value_is_literal": True}
        )
