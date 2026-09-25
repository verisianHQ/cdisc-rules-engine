from typing import List

from cdisc_rules_engine.constants.classes import ASSOCIATED_PERSONS
from cdisc_rules_engine.constants.domains import AP_DOMAIN
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.utilities import sdtm_utilities


class SqlGetModelColumnOrder(SqlBaseOperation):

    def _execute_operation(self):

        model_variables = self._get_model_variables()

        query = self._format_variable_list_to_query(vars=model_variables)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_model_variables(self):
        try:
            related_domain = self.get_ap_related_domain()
            domain = related_domain or self.params.domain

            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(domain)

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(model_variables, domain)

            if related_domain:
                model_details = self.params.standards_context.get_model_metadata()
                class_details = sdtm_utilities.get_class_metadata(model_details, ASSOCIATED_PERSONS)
                ap_variables = class_details.get("classVariables", [])
                ap_names = self._replace_variable_wildcards(ap_variables, AP_DOMAIN)

                existing = set(variable_names_list)
                variable_names_list += [name for name in ap_names if name not in existing]

            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")
