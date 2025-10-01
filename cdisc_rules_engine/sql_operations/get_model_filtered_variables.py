from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.utilities import sdtm_utilities
from typing import List


class SqlGetModelFilteredVariablesOperation(SqlBaseOperation):

    def _execute_operation(self):
        """
        Fetches variables from the CDISC library model that match the specified filter criteria.
        Similar to LibraryModelVariablesFilter but for SQL operations.

        Filters variables based on key_name and key_value parameters.
        For example: key_name="role", key_value="Timing" would return timing variables.

        Returns a SQL query that produces the filtered variable names as a constant array.
        """

        # Get the filter criteria
        key = self.params.key_name
        val = self.params.key_value

        if not key or not val:
            # Return empty array if no filter criteria provided
            query = "SELECT ARRAY[]::TEXT[] AS value"
            return SqlOperationResult(query=query, type="constant", subtype="Array[String]")

        # Get model variables and filter them
        model_variables = self._get_model_filtered_variables()

        # Convert the list to a SQL array
        if model_variables:
            # Format variable names for SQL array literal
            formatted_vars = [f"'{var}'" for var in model_variables]
            variables_array = f"ARRAY[{','.join(formatted_vars)}]"
            query = f"SELECT {variables_array}::TEXT[] AS value"
        else:
            query = "SELECT ARRAY[]::TEXT[] AS value"

        return SqlOperationResult(query=query, type="constant", subtype="Array[String]")

    def _get_model_filtered_variables(self):
        """
        Get variables metadata from standard model and filter by key_name/key_value.

        This is the SQL equivalent of the original operation's _get_model_filtered_variables method.
        """
        key = self.params.key_name
        val = self.params.key_value

        try:
            # Use the new SQL base operation method
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)
            filtered_model = [var for var in model_variables if var.get(key) == val]
            # Use the correct signature for replace_variable_wildcards
            variable_names_list = []
            sdtm_utilities.replace_variable_wildcards(filtered_model, self.params.domain, variable_names_list)
            return variable_names_list

        except Exception as e:
            return f"Error retrieving model variables: {str(e)}"
