from typing import List
from cdisc_rules_engine.constants.permissibility import REQUIRED
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.utilities import sdtm_utilities


class SqlRequiredVariablesOperation(SqlBaseOperation):
    def _execute_operation(self):
        variables_metadata: List[dict] = sdtm_utilities.get_variables_metadata_from_standard(
            self.params.domain, self.library_metadata
        )

        required_vars = []
        for var in variables_metadata:
            if self._get_allowed_variable_permissibility(var) == REQUIRED:
                var_name = var["name"].replace("--", self.params.domain)
                required_vars.append(var_name)

        if not required_vars:
            return SqlOperationResult(query="SELECT NULL AS value WHERE FALSE", type="collection", subtype="Char")

        table_values_clause = ", ".join([f"('{name}')" for name in required_vars])
        query = f"SELECT column1 AS value FROM (VALUES {table_values_clause})"

        return SqlOperationResult(query=query, type="collection", subtype="Char")
