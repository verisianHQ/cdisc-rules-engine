from typing import Literal, Tuple

from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlSetOperationBase(SqlBaseOperation):
    """
    Shared operand resolution for binary set operations over 'name' and 'subtract' - each of
    which may be a column, a literal list, or a reference to a previous collection/constant
    operation.
    """

    def _resolve_operand_queries(self, domain: str, dataset_id: str) -> Tuple[str, str, dict]:
        """Resolve the 'name' and 'subtract' operands to (name_query, subtract_query, params)."""
        name_param = self.params.name
        subtract_param = self.params.subtract

        name_query = self._remap_params(self._resolve_param(name_param, domain, dataset_id, "query"), "name")
        subtract_query = self._remap_params(
            self._resolve_param(subtract_param, domain, dataset_id, "query"), "subtract"
        )

        name_params = self._remap_params(self._resolve_param(name_param, domain, dataset_id, "param"), "name")
        subtract_params = self._remap_params(
            self._resolve_param(subtract_param, domain, dataset_id, "param"), "subtract"
        )

        if not isinstance(name_params, dict):
            name_params = {}
        if not isinstance(subtract_params, dict):
            subtract_params = {}

        return name_query, subtract_query, {**name_params, **subtract_params}

    def _resolve_param(self, param_val: str, domain: str, dataset_id: str, q_or_p: Literal["query", "param"]) -> str:
        if self._column_exists_in_domain(domain, param_val):
            col_hash = self.data_service.pgi.schema.get_column_hash(domain, param_val)
            return f"SELECT {col_hash} AS value FROM {dataset_id} WHERE {col_hash} IS NOT NULL"
        elif isinstance(param_val, list):
            return self._format_variable_list_to_query(vars=param_val, unique=True)
        elif self._get_previous_operation(param_val):
            return (
                self._get_previous_operation(param_val).query
                if q_or_p == "query"
                else self._get_previous_operation(param_val).params
            )
        else:
            return {}

    def _remap_params(self, element: str | dict, param_name: str) -> str:
        if not element:
            return {}
        if isinstance(element, dict):
            return {self._remap_params(k, param_name): v for k, v in element.items()}
        else:
            return element.replace("$", f"${param_name}_")
