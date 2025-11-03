from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlPermissibilityOperation(SqlBaseOperation):

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, permissibility: str):
        super().__init__(params, data_service)
        self.permissibility = permissibility

    def _execute_operation(self):

        permissible_variables = self._get_permissible_variables()

        # Convert the list to individual rows in SQL
        if permissible_variables and isinstance(permissible_variables, list):
            # Format variable names for SQL VALUES clause, escaping single quotes
            formatted_vars = [f"('{var.replace(chr(39), chr(39) + chr(39))}')" for var in permissible_variables]
            values_clause = ", ".join(formatted_vars)
            query = f"SELECT column1 AS value FROM (VALUES {values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows - this is a valid empty table
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_permissible_variables(self):
        try:
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)

            # Filter variables by 'permissible' permissibility
            perm_model = [
                var for var in model_variables if self._get_allowed_variable_permissibility(var) == self.permissibility
            ]

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(perm_model, self.params.domain)

            # Extract just the variable names from the processed metadata
            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")
