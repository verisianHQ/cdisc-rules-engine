from .base_sql_operator import BaseSqlOperator


class IsOrderedSubsetOfOperator(BaseSqlOperator):
    """Operator for checking if any array (column, list, or collection) is an ordered subset of another array."""

    def execute_operator(self, other_value):
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        if target is None or comparator is None:
            raise ValueError("Missing required parameters: target or comparator")

        prefix = other_value.get("prefix", None)
        suffix = other_value.get("suffix", None)

        target = self.replace_prefix(target) if isinstance(target, str) else target
        comparator = self.replace_prefix(comparator) if isinstance(comparator, str) else comparator

        def sql():
            target_cte = self._build_sequence_cte(target, prefix=prefix, suffix=suffix)
            comparator_cte = self._build_sequence_cte(comparator)

            return f"""
            WITH target_seq AS (
                {target_cte}
            ),
            comparator_seq AS (
                {comparator_cte}
            ),
            joined AS (
                SELECT t.val, t.idx AS t_idx, MIN(c.idx) AS c_idx
                FROM target_seq t
                LEFT JOIN comparator_seq c ON t.val = c.val
                GROUP BY t.val, t.idx
            )
            SELECT CASE
                WHEN NOT EXISTS (SELECT 1 FROM target_seq) THEN TRUE
                WHEN EXISTS (SELECT 1 FROM joined WHERE c_idx IS NULL) THEN FALSE
                WHEN EXISTS (
                    SELECT 1
                    FROM joined j1
                    JOIN joined j2 ON j1.t_idx < j2.t_idx
                    WHERE j1.c_idx >= j2.c_idx
                ) THEN FALSE
                ELSE TRUE
            END
            """

        return self._do_check_operator(sql)

    def _build_sequence_cte(self, value, prefix=None, suffix=None):
        if isinstance(value, list):
            elements = ", ".join(self._constant_sql(v) for v in value)
            return f"""
                SELECT val::text, ordinality AS idx
                FROM UNNEST(ARRAY[{elements}]) WITH ORDINALITY AS t(val, ordinality)
                WHERE val IS NOT NULL AND val::text != ''
            """

        elif isinstance(value, str):
            if value in self.operation_variables:
                var = self.operation_variables[value]
                if var.type == "collection":
                    coll_sql = self._collection_sql(value)
                    return f"""
                        SELECT value::text AS val, ROW_NUMBER() OVER () AS idx
                        FROM {coll_sql} AS coll_values(value)
                        WHERE value IS NOT NULL AND value::text != ''
                    """
                else:
                    const_sql = self._constant_sql(value)
                    return f"""
                        SELECT {const_sql}::text AS val, 1 AS idx
                        WHERE {const_sql} IS NOT NULL AND {const_sql}::text != ''
                    """
            elif self._exists(value):
                col_sql = self._column_sql(value, alias=False, prefix=prefix, suffix=suffix)
                table_sql = self._table_sql()
                return f"""
                    SELECT {col_sql}::text AS val, ROW_NUMBER() OVER(ORDER BY id) AS idx
                    FROM {table_sql}
                    WHERE {col_sql} IS NOT NULL AND {col_sql}::text != ''
                """

        else:
            const_sql = self._constant_sql(value)
            return f"""
                SELECT {const_sql}::text AS val, 1 AS idx
                WHERE {const_sql} IS NOT NULL AND {const_sql}::text != ''
            """
