from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetDatasetFilteredVariables(SqlBaseOperation):

    def _execute_operation(self):
        """
        Fetches variables from the dataset that match the specified filter criteria.
        Filters variables based on key_name and key_value parameters.
        """
        model_variables = self._get_model_filtered_variables()
        ds_variables = self._get_dataset_variables()
        intersection = list(set(model_variables).intersection(set(ds_variables)))
        query = self._format_variable_list_to_query(vars=intersection)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_model_filtered_variables(self):
        key = self.params.key_name
        val = self.params.key_value

        if not key or not val:
            return []

        model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)
        filtered_model = [var for var in model_variables if var.get(key) == val]
        variable_names_list = self._replace_variable_wildcards(filtered_model, self.params.domain)
        return variable_names_list

    def _get_dataset_variables(self):
        all_dataset_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        ds_metadata = next((md for md in all_dataset_metadata if md.domain == self.params.domain), None)
        return [var.name for var in ds_metadata.variables] if ds_metadata else []
