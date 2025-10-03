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

        NOTE: This operator performs DATASET-LEVEL analysis, not row-level analysis.
        The comparison is done once at the dataset level (e.g., comparing all unique values
        in a target column with all unique values in a comparator column), but returns
        a consistent truth series for each row to maintain compatibility with the SQL
        engine execution framework.

        Operation types:
        - 'no_elements': Returns True for each row if target and comparator share no elements at dataset level
        - 'at_least_one': Returns True for each row if target and comparator share at least one element at dataset level
        - 'exactly_one': Returns True for each row if target and comparator share exactly one element at dataset level

        The dataset-level comparison converts both target and comparator values to sets:
        - If the value is a column, uses all unique non-empty values from that column
        - If the value is a collection operation variable, uses all values from the collection
        - If the value is a single element/constant, treats it as a single-element set

        Both target and comparator can be:
        - Column names (dataset-level: all unique values in the column)
        - Operation variables (constant or collection type)

        Returns:
            pd.Series: A pandas Series of booleans with the same value for each row in the dataset
                      (the dataset-level comparison result replicated for all rows)
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        if not all([target, comparator]):
            raise ValueError("Missing required parameters: target or comparator")

        target = self.replace_prefix(target) if isinstance(target, str) else target
        comparator = self.replace_prefix(comparator) if isinstance(comparator, str) else comparator

        cache_key = f"{target}_shares_elements_{self.operation_type}_{comparator}"

        query = self._build_result_query(target, comparator)
        self.sql_data_service.pgi.execute_sql(query)
        results = self.sql_data_service.pgi.fetch_all()
        result_value = results[0]["result"] if results else False

        def sql():
            return "TRUE" if result_value else "FALSE"

        return self._do_check_operator(cache_key, sql)

    def _build_result_query(self, target, comparator):
        """Build a query that computes the actual boolean result for the operation."""
        query_expression = self._build_shares_elements_query(target, comparator)
        return f"SELECT ({query_expression}) AS result FROM {self._table_sql()} AS co LIMIT 1"

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
        """Build the appropriate SQL query based on operation variable types."""
        if not (isinstance(target, str) and target in self.operation_variables):
            raise ValueError(f"Target '{target}' is not an operation variable")
        if not (isinstance(comparator, str) and comparator in self.operation_variables):
            raise ValueError(f"Comparator '{comparator}' is not an operation variable")

        # Determine if they are collection or constant operation variables
        target_is_collection = self.operation_variables[target].type == "collection"
        comparator_is_collection = self.operation_variables[comparator].type == "collection"

        # Handle collection vs collection
        if target_is_collection and comparator_is_collection:
            return self._build_collection_vs_collection_query(target, comparator)

        # Handle collection vs constant
        elif target_is_collection:
            comparator_sql = self._sql(comparator)
            comparator_empty_sql = self._is_empty_sql(comparator, alias=False)
            return self._build_collection_vs_value_query(target, comparator_sql, comparator_empty_sql, True)

        elif comparator_is_collection:
            target_sql = self._sql(target)
            target_empty_sql = self._is_empty_sql(target, alias=False)
            return self._build_collection_vs_value_query(comparator, target_sql, target_empty_sql, False)

        # Handle constant vs constant operation variables
        else:
            target_sql = self._sql(target)
            comparator_sql = self._sql(comparator)
            return self._build_simple_vs_simple_query(target, target_sql, comparator, comparator_sql)

    def _build_simple_vs_simple_query(self, target, target_sql, comparator, comparator_sql):
        """Build query for constant operation variable vs constant operation variable comparison."""
        # Generate proper empty checks for operation variables
        target_empty_sql = self._is_empty_sql(target, alias=False)
        comparator_empty_sql = self._is_empty_sql(comparator, alias=False)

        if self.operation_type == "no_elements":
            return f"""
                CASE
                    WHEN ({target_empty_sql}) OR ({comparator_empty_sql}) THEN TRUE
                    ELSE {target_sql} != {comparator_sql}
                END
            """
        elif self.operation_type == "at_least_one":
            return f"""
                CASE
                    WHEN ({target_empty_sql}) OR ({comparator_empty_sql}) THEN FALSE
                    ELSE {target_sql} = {comparator_sql}
                END
            """
        elif self.operation_type == "exactly_one":
            # For simple values, exactly_one is the same as at_least_one
            # since there can only be 0 or 1 shared element when comparing single values
            return f"""
                CASE
                    WHEN ({target_empty_sql}) OR ({comparator_empty_sql}) THEN FALSE
                    ELSE {target_sql} = {comparator_sql}
                END
            """

    def _build_collection_vs_collection_query(self, target_var, comparator_var):
        """Build query for collection vs collection comparison using base class methods."""
        target_collection_sql = self._collection_sql(target_var)
        comparator_collection_sql = self._collection_sql(comparator_var)

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            )
            """
        elif self.operation_type == "exactly_one":
            return f"""
            (
                SELECT COUNT(DISTINCT target_values.value)
                FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE target_values.value IS NOT NULL AND target_values.value != ''
                AND comparator_values.value IS NOT NULL AND comparator_values.value != ''
            ) = 1
            """

    def _build_collection_vs_value_query(self, collection_var, value_sql, value_empty_sql, target_is_collection):
        """Build query for collection operation variable vs constant operation variable comparison."""
        collection_sql = self._collection_sql(collection_var)

        if self.operation_type == "no_elements":
            return f"""
                CASE
                    WHEN ({value_empty_sql}) THEN TRUE
                    ELSE NOT EXISTS (
                        SELECT 1 FROM {collection_sql} AS collection_values(value)
                        WHERE collection_values.value = {value_sql}
                        AND collection_values.value IS NOT NULL AND collection_values.value != ''
                    )
                END
            """
        elif self.operation_type == "at_least_one":
            return f"""
                CASE
                    WHEN ({value_empty_sql}) THEN FALSE
                    ELSE EXISTS (
                        SELECT 1 FROM {collection_sql} AS collection_values(value)
                        WHERE collection_values.value = {value_sql}
                        AND collection_values.value IS NOT NULL AND collection_values.value != ''
                    )
                END
            """
        elif self.operation_type == "exactly_one":
            return f"""
                CASE
                    WHEN ({value_empty_sql}) THEN FALSE
                    ELSE (
                        SELECT COUNT(*)
                        FROM {collection_sql} AS collection_values(value)
                        WHERE collection_values.value = {value_sql}
                        AND collection_values.value IS NOT NULL AND collection_values.value != ''
                    ) = 1
                END
            """
