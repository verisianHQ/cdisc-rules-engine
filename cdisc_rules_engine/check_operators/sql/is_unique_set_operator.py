from .base_sql_operator import BaseSqlOperator


class IsUniqueSetOperator(BaseSqlOperator):
    """Operator for checking if values form a unique set."""

    def execute_operator(self, other_value):
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        all_columns = []

        items_to_process = [target, comparator]
        while items_to_process:
            item = items_to_process.pop(0)
            if isinstance(item, list):
                items_to_process = item + items_to_process
            elif item:
                all_columns.append(item)

        seen = set()
        unique_columns = []

        for col_raw in all_columns:
            clean_name = self.replace_prefix(col_raw).lower()
            clean_col = clean_name if self._exists(clean_name) else None
            if clean_col and clean_col not in seen:
                seen.add(clean_col)
                unique_columns.append(clean_col)

        if not unique_columns:
            return self._do_check_operator("is_unique_set_no_cols", lambda: "TRUE")

        op_name = f"{'_'.join(unique_columns)}_is_unique_set"

        def generate_update_query(db_table: str, db_column: str) -> str:
            concat_parts = [f"COALESCE(CAST({self._column_sql(col)} AS TEXT), '_NULL_')" for col in unique_columns]
            concat_expr = " || '|' || ".join(concat_parts)

            return f"""
                UPDATE {db_table} AS t
                SET {db_column} = sub.is_unique
                FROM (
                    SELECT
                        id,
                        CASE
                            WHEN COUNT(*) OVER (
                                PARTITION BY {concat_expr}
                            ) <= 1
                            THEN TRUE
                            ELSE FALSE
                        END AS is_unique
                    FROM {db_table}
                    ORDER BY id
                ) AS sub
                WHERE t.id = sub.id;
            """

        return self._do_complex_check_operator(op_name, generate_update_query)
