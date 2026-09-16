from collections import OrderedDict

from cdisc_rules_engine.constants.classes import DETECTABLE_CLASSES
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetColumnOrderFromLibrary(SqlBaseOperation):

    def _execute_operation(self):
        """
        Fetches column order for a given domain from the CDISC library.

        The list of column names is sorted in accordance with the "ordinal" key of the
        library metadata. Optionally filters variables based on specified metadata criteria
        (key_name/key_value, e.g. key_name="role", key_value="Timing") before the names are
        extracted.
        """
        library_variables = self._get_library_column_order_variables()

        query = self._format_variable_list_to_query(vars=library_variables)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_library_column_order_variables(self) -> List[str]:

        domain = self.params.domain

        variables_metadata: List[dict] = self._get_variables_metadata_from_standard(domain)

        # Some variables (eg generic Timing variables like --DTC/--ENRTPT) are defined
        # once at the model level and aren't always relisted in IG,
        # so an IG-only fetch can miss columns. Merge in model's
        # variables for domain classes where the model applies
        if self.get_dataset_class(domain) in DETECTABLE_CLASSES:
            model_variables = self._get_variables_metadata_from_standard_model(domain)
            variables_metadata = self._merge_model_variables(variables_metadata, model_variables, domain)

        variables_metadata = self._filter_by_metadata_criteria(variables_metadata)

        variable_names_list = self._replace_variable_wildcards(variables_metadata, domain)

        # dedupe preserving order
        return list(OrderedDict.fromkeys(variable_names_list))

    @staticmethod
    def _merge_model_variables(ig_variables: List[dict], model_variables: List[dict], domain: str) -> List[dict]:
        """Use the model's variable order as the authoritative sequence, merging in IG-only extras by role."""

        def resolved_name(var: dict) -> str:
            return (var.get("name") or "").replace("--", domain)

        ig_by_resolved_name = {resolved_name(var): var for var in ig_variables}

        merged = []
        seen_resolved_names = set()
        for model_var in model_variables:
            name = resolved_name(model_var)
            seen_resolved_names.add(name)
            merged.append(ig_by_resolved_name.get(name, model_var))

        for ig_var in ig_variables:
            name = resolved_name(ig_var)
            if name in seen_resolved_names:
                continue
            seen_resolved_names.add(name)

            # Identifiers at the front, Timing at the end, everything else just before the Timing section
            role = ig_var.get("role")
            if role == "Identifier":
                insertion_point = sum(1 for v in merged if v.get("role") == "Identifier")
            elif role == "Timing":
                insertion_point = len(merged)
            else:
                timing_count = sum(1 for v in merged if v.get("role") == "Timing")
                insertion_point = len(merged) - timing_count
            merged.insert(insertion_point, ig_var)

        return merged

    def _filter_by_metadata_criteria(self, variables_metadata: List[dict]) -> List[dict]:
        """Optionally filter variables by a key_name/key_value metadata criterion."""
        key = self.params.key_name
        val = self.params.key_value
        if not key or not val:
            return variables_metadata

        return [var for var in variables_metadata if var.get(key) == val]
