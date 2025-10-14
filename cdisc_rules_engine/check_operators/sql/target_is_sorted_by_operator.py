from .base_sql_operator import BaseSqlOperator


class TargetIsSortedByOperator(BaseSqlOperator):
    """Operator for checking if target is sorted by specified criteria."""

    def _is_invalid_date_sql(self, date_column):
        """
        Check if a date is invalid using simple SQL logic.
        Returns SQL expression that evaluates to TRUE if the date is invalid, FALSE if valid.
        """
        return f"""NOT (
                -- Valid ISO 8601 formats
                {date_column} ~ '^[0-9]{{4}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' OR
                -- Uncertainty patterns
                {date_column} ~ '^[0-9]{{4}}--$' OR
                {date_column} ~ '^[0-9]{{4}}-[0-9]{{2}}--$' OR
                {date_column} ~ '^[0-9]{{4}}----$'
            )"""

    def execute_operator(self, other_value):
        """
        Checks if target values are sorted correctly based on comparator columns.

        For each group (defined by 'within'), verifies that target values follow
        the expected order when rows are sorted by comparator columns. Also handles
        date overlap validation for partial dates.

        Args:
            other_value: Dictionary containing:
                - target: The target column to check sorting of
                - within: The column to group by
                - comparator: List of dictionaries with:
                    - name: Column name to sort by
                    - sort_order: "ASC" or "DESC"
                    - null_position: "first" or "last"

        Returns:
            Boolean series indicating if each record meets the sorting condition
        """
        target = self.replace_prefix(other_value.get("target"))
        within = self.replace_prefix(other_value.get("within"))
        comparators = other_value["comparator"]

        if not all([target, within, comparators]):
            raise ValueError("Missing required parameters: target, within, or comparator")

        comparator_parts = []
        for comp in comparators:
            name = self.replace_prefix(comp["name"])
            order = comp["sort_order"].upper()
            null_pos = comp["null_position"]
            comparator_parts.append(f"{name}_{order}_{null_pos}")

        cache_key = f"{target}_is_sorted_by_{'_'.join(comparator_parts)}_within_{within}"

        def sql(table_name, column_name):
            order_by_parts = []
            for i, comp in enumerate(comparators):
                comp_name = self.replace_prefix(comp["name"])
                sort_order = comp["sort_order"].upper()
                null_pos = comp["null_position"].upper()

                comp_sql = self._column_sql(comp_name, alias=False)
                order_part = f"{comp_sql} {sort_order}"
                if null_pos == "FIRST":
                    order_part += " NULLS FIRST"
                else:
                    order_part += " NULLS LAST"
                order_by_parts.append(order_part)

            order_by_clause = ", ".join(order_by_parts)

            # Get first comparator for checking NULL values and null positioning
            first_comp = comparators[0]
            first_comp_name = self.replace_prefix(first_comp["name"])
            first_comp_sql = self._column_sql(first_comp_name, alias=False)
            first_comp_null_pos = first_comp["null_position"].upper()

            # Build target ordering that respects the same null positioning as comparators
            first_comp_null_pos = first_comp["null_position"].upper()
            if first_comp_null_pos == "FIRST":
                target_order_clause = "target_val NULLS FIRST"
            else:
                target_order_clause = "target_val NULLS LAST"

            return f"""
            -- Check if target is sorted correctly by comparator columns within groups
            WITH sorted_with_positions AS (
                SELECT
                    id,
                    {self._column_sql(target, alias=False)} AS target_val,
                    {self._column_sql(within, alias=False)} AS within_val,
                    {first_comp_sql} AS comp_val,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._column_sql(within, alias=False)}
                        ORDER BY {order_by_clause}
                    ) AS sorted_position
                FROM {table_name}
            ),
            target_order AS (
                SELECT
                    id,
                    target_val,
                    within_val,
                    comp_val,
                    sorted_position,
                    ROW_NUMBER() OVER (
                        PARTITION BY within_val
                        ORDER BY {target_order_clause}
                    ) AS target_sorted_position
                FROM sorted_with_positions
            ),
            basic_check AS (
                SELECT
                    id,
                    CASE
                        -- If comparator is non-null but invalid date, always mark as False
                        WHEN comp_val IS NOT NULL AND ({self._is_invalid_date_sql("comp_val")}) THEN false
                        -- Check if the positions match (target order = expected order)
                        ELSE sorted_position = target_sorted_position
                    END AS is_valid
                FROM target_order
            ),
            date_overlap_check AS (
                SELECT
                    s1.id,
                    CASE
                        -- Use invalid_date operator logic to check if dates are valid before checking overlaps
                        WHEN ({self._is_invalid_date_sql("s1.comp_val")}) OR
                             ({self._is_invalid_date_sql("s2.comp_val")}) THEN true
                        WHEN s1.comp_val ~ '^[0-9]{{4}}$' AND s2.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}'
                             AND s2.comp_val LIKE s1.comp_val || '%' THEN false
                        WHEN s1.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}$'
                             AND s2.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                             AND s2.comp_val LIKE s1.comp_val || '%' THEN false
                        WHEN s2.comp_val ~ '^[0-9]{{4}}$' AND s1.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}'
                             AND s1.comp_val LIKE s2.comp_val || '%' THEN false
                        WHEN s2.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}$'
                             AND s1.comp_val ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                             AND s1.comp_val LIKE s2.comp_val || '%' THEN false
                        ELSE true
                    END AS date_overlap_ok
                FROM sorted_with_positions s1
                LEFT JOIN sorted_with_positions s2 ON s1.within_val = s2.within_val
                    AND s2.sorted_position = s1.sorted_position + 1
            )
            UPDATE {table_name} t
            SET {column_name} = (
                SELECT (b.is_valid AND d.date_overlap_ok)
                FROM basic_check b
                JOIN date_overlap_check d ON b.id = d.id
                WHERE b.id = t.id
            )
            """

        return self._do_complex_check_operator(cache_key, sql)
