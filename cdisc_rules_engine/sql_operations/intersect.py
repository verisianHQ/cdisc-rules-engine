from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.set_operation_base import SqlSetOperationBase


class SqlIntersectOperation(SqlSetOperationBase):
    def _execute_operation(self):
        """
        Executes a set intersection operation - the inverse of minus/EXCEPT.
        Returns the values present in BOTH 'name' and 'subtract', preserving name's
        original order.
        """
        domain = self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(domain)
        case_sensitive = self.params.case_sensitive

        name_query, subtract_query, params = self._resolve_operand_queries(domain, dataset_id)

        case_value = "UPPER(value)" if not case_sensitive else "value"
        subtract_case_value = "UPPER(subtract_q.value)" if not case_sensitive else "subtract_q.value"

        # changed to use EXISTS to preserve order and handle nulls correctly
        query = f"""
            SELECT value FROM (
                SELECT value, MIN(ord) AS first_ord FROM (
                    SELECT value, ord FROM (
                        SELECT {case_value} AS value, ROW_NUMBER() OVER () AS ord
                        FROM ({name_query}) AS name_q
                    ) AS n
                    WHERE EXISTS (
                        SELECT 1 FROM ({subtract_query}) AS subtract_q
                        WHERE {subtract_case_value} = n.value
                    )
                ) AS filtered
                GROUP BY value
            ) AS deduped
            ORDER BY first_ord
        """

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=params or None)
