from .base_sql_operator import BaseSqlOperator


class SharesElementsWithOperator(BaseSqlOperator):
    """Unified operator for checking if values share elements with different comparison modes."""

    def __init__(self, data, operation_type="no_elements"):
        """Initialize the operator with a specific operation type.

        Args:
            data: The data service and other initialization parameters
            operation_type: One of 'no_elements', 'at_least_one', or 'exactly_one'
        """
        super().__init__(data)
        self.operation_type = operation_type

    def execute_operator(self, other_value):
        """
        Checks if values share elements according to the operation type.

        Operation types:
        - 'no_elements': Returns True if ALL rows have no shared elements
        - 'at_least_one': Returns True if ANY row has at least one shared element
        - 'exactly_one': Returns True if ANY row has exactly one shared element

        For each row, converts both target and comparator values to sets:
        - If the value is a list/array, converts directly to set
        - If the value is a single element, wraps it in a set

        Both target and comparator can be:
        - Column names
        - Operation variables (constant or collection type)
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        if not all([target, comparator]):
            raise ValueError("Missing required parameters: target or comparator")

        target = self.replace_prefix(target) if isinstance(target, str) else target
        comparator = self.replace_prefix(comparator) if isinstance(comparator, str) else comparator

        # Build and execute the appropriate query based on value types
        query = self._build_shares_elements_query(target, comparator)

        self.sql_data_service.pgi.execute_sql(query)
        result = self.sql_data_service.pgi.fetch_one()
        return result["result"]

    def _is_collection_variable(self, value):
        """Check if a value is a collection-type operation variable.

        Args:
            value: The value to check

        Returns:
            bool: True if the value is a collection variable, False otherwise
        """
        return (
            isinstance(value, str)
            and value in self.operation_variables
            and self.operation_variables[value].type == "collection"
        )

    def _build_shares_elements_query(self, target, comparator):
        """Build the appropriate SQL query based on value types."""
        # Determine value types for routing
        target_is_collection = self._is_collection_variable(target)
        comparator_is_collection = self._is_collection_variable(comparator)

        # Handle collection vs collection
        if target_is_collection and comparator_is_collection:
            return self._build_collection_vs_collection_query(target, comparator)

        # Handle collection vs simple value
        elif target_is_collection:
            comparator_sql = self._sql(comparator)
            comparator_empty_sql = self._is_empty_sql(comparator, alias=False)
            return self._build_collection_vs_value_query(target, comparator_sql, comparator_empty_sql, True)

        elif comparator_is_collection:
            target_sql = self._sql(target)
            target_empty_sql = self._is_empty_sql(target, alias=False)
            return self._build_collection_vs_value_query(comparator, target_sql, target_empty_sql, False)

        # Handle simple value vs simple value
        else:
            target_sql = self._sql(target)
            comparator_sql = self._sql(comparator)
            return self._build_simple_vs_simple_query(target, target_sql, comparator, comparator_sql)

    def _build_simple_vs_simple_query(self, target, target_sql, comparator, comparator_sql):
        """Build query for simple value vs simple value comparison."""
        target_empty_sql = self._is_empty_sql(target, alias=False)
        comparator_empty_sql = self._is_empty_sql(comparator, alias=False)

        if self.operation_type == "no_elements":
            return f"""
            SELECT NOT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({target_empty_sql})
                AND NOT ({comparator_empty_sql})
                AND {target_sql} = {comparator_sql}
            ) as result
            """
        else:  # at_least_one or exactly_one
            return f"""
            SELECT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({target_empty_sql})
                AND NOT ({comparator_empty_sql})
                AND {target_sql} = {comparator_sql}
            ) as result
            """

    def _build_collection_vs_collection_query(self, target_var, comparator_var):
        """Build query for collection vs collection comparison using base class methods."""
        target_collection_sql = self._collection_sql(target_var)
        comparator_collection_sql = self._collection_sql(comparator_var)

        if self.operation_type == "no_elements":
            return f"""
            SELECT NOT EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            ) as result
            """
        elif self.operation_type == "at_least_one":
            return f"""
            SELECT EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            ) as result
            """
        elif self.operation_type == "exactly_one":
            return f"""
            SELECT EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            ) as result
            """

    def _build_collection_vs_value_query(self, collection_var, value_sql, value_empty_sql, target_is_collection):
        """Build query for collection vs single value comparison."""
        collection_sql = self._collection_sql(collection_var)

        if self.operation_type == "no_elements":
            return f"""
            SELECT NOT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({value_empty_sql})
                AND EXISTS (
                    SELECT 1 FROM {collection_sql} AS collection_values(value)
                    WHERE collection_values.value = {value_sql}
                    AND collection_values.value IS NOT NULL AND collection_values.value != ''
                )
            ) as result
            """
        elif self.operation_type == "at_least_one":
            return f"""
            SELECT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({value_empty_sql})
                AND EXISTS (
                    SELECT 1 FROM {collection_sql} AS collection_values(value)
                    WHERE collection_values.value = {value_sql}
                    AND collection_values.value IS NOT NULL AND collection_values.value != ''
                )
            ) as result
            """
        elif self.operation_type == "exactly_one":
            return f"""
            SELECT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({value_empty_sql})
                AND EXISTS (
                    SELECT 1 FROM {collection_sql} AS collection_values(value)
                    WHERE collection_values.value = {value_sql}
                    AND collection_values.value IS NOT NULL AND collection_values.value != ''
                )
            ) as result
            """
