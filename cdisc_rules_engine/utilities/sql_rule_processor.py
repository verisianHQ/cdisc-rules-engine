import re
from copy import deepcopy
from itertools import product
from typing import List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.dataset_metadata2 import VariableMetadata
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


class SQLRuleProcessor:
    REGEX_CAPTURE_REFERENCE = re.compile(r"\{\{(?P<name>[A-Za-z_]\w*)\}\}")

    # @staticmethod
    # def _ct_package_type_api_name(ct_package_type: str | None) -> str:
    #     if ct_package_type is None:
    #         return None
    #     return f"{ct_package_type.lower()}ct"

    @staticmethod
    def perform_rule_operations(
        rule: dict,
        dataset_id: str,
        dataset_metadata: BaseDatasetMetadata,
        data_service: PostgresQLDataService,
        standards_context: BaseStandardsContext,
    ) -> dict[str, SqlOperationResult]:
        """
        Each operation creates an output variable
        """
        operations: List[dict] = rule.get("operations") or []
        output_variables: dict[str, SqlOperationResult] = {}

        for operation in operations:
            rule_name = operation.get("operator", "")
            output_variable = operation.get("id", "")

            if not output_variable.startswith("$"):
                raise ValueError(
                    f"Output variable must start with '$', "
                    f"but got '{output_variable}' in rule {rule.get('core_id', 'unknown')}"
                )

            # change -- pattern to domain name
            target_variable: str = operation.get("name", None)
            operation_domain: str = operation.get("domain", dataset_metadata.domain)
            operation_table = dataset_id
            if target_variable:
                target_variable = standards_context.replace_domain_code(dataset_metadata, target_variable)

            # build parameters for the operation
            params = SqlOperationParams(
                domain=operation_domain,
                target=target_variable,
                standards_context=standards_context,
                name=operation.get("name"),
                previous_operations=output_variables,
                grouping=operation.get("group"),
                filter=operation.get("filter"),
                key_name=operation.get("key_name"),
                key_value=operation.get("key_value"),
                ct_package_types=operation.get("ct_package_types"),
                ct_version=operation.get("version"),
                ct_attribute=operation.get("ct_attribute"),
                ct_conditions=operation.get("ct_conditions"),
                attribute_name=operation.get("attribute_name"),
                subtract=operation.get("subtract"),
                external_dictionary_type=operation.get("external_dictionary_type"),
                codelist=operation.get("codelist"),
                domain_class=operation.get("domain_class"),
                case_sensitive=operation.get("case_sensitive", True),
                table=operation_table,
                use_rule_type_table=operation.get("use_rule_type_table", False),
                value=operation.get("value"),
            )

            operation = SqlOperationsFactory.get_service(rule_name, params=params, data_service=data_service)
            query = operation.execute()
            output_variables[output_variable] = query

            logger.info(f"Processed rule operation. " f"operation={rule_name}, rule={rule}")
        return output_variables

    # def is_relationship_dataset(self, dataset_name: str) -> bool:
    #     # TODO: this should come from the library and from the dataset metadata
    #     if dataset_name in ["RELREC", "RELSUB", "CO"]:
    #         result = True
    #     elif dataset_name.startswith("SUPP"):
    #         result = True
    #     elif dataset_name.startswith("SQ"):
    #         result = True
    #     else:
    #         result = False
    #     # logger.info(f"is_relationship_dataset. dataset_name={dataset_name}, result={result}")
    #     return result

    # def get_size_unit_from_rule(self, rule: dict) -> Optional[str]:
    #     """
    #     Extracts size unit from rule if it was passed
    #     """
    #     rule_conditions: ConditionInterface = rule["conditions"]
    #     for condition in rule_conditions.values():
    #         value: dict = condition["value"]
    #         if value["target"] == "dataset_size":
    #             return value.get("unit")

    # def add_operator_to_rule_conditions(self, rule: dict, target_to_operator_map: dict, domain: str):
    #     """
    #     Adds "operator" key to rule condition.
    #     target_to_operator_map parameter is a dict
    #     where keys are targets and values are operators.

    #     The rule is passed and changed by reference.
    #     """
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         target: str = condition.get("value", {}).get("target", "").replace("--", domain)
    #         operator_to_add: Optional[Union[str, list]] = target_to_operator_map.get(target)
    #         if not operator_to_add:
    #             continue
    #         if isinstance(operator_to_add, str):
    #             condition["operator"] = operator_to_add
    #         elif isinstance(operator_to_add, list):
    #             nested_conditions = [{**condition, "operator": operator} for operator in operator_to_add]
    #             condition.clear()  # delete all keys from dict
    #             condition[AllowedConditionsKeys.ANY.value] = nested_conditions

    # def add_comparator_to_rule_conditions(self, rule: dict, comparator: dict = None, target_prefix=None):
    #     """
    #     Adds "comparator" key to rule conditions.value key.

    #     comparator parameter is a dict where
    #     keys are targets and values are comparators.

    #     The rule is passed and changed by reference.
    #     """
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         value: dict = condition["value"]
    #         if comparator:
    #             # Adding a specific value
    #             comparator_to_add = comparator.get(value["target"])
    #         elif target_prefix:
    #             # Referencing a target variable in another dataset
    #             comparator_to_add = f"{target_prefix}{value['target']}"
    #         else:
    #             comparator_to_add = None
    #         if comparator_to_add:
    #             value["comparator"] = comparator_to_add
    #     # logger.info(
    #         f"Added comparator to rule conditions. " f"comparator={comparator}, conditions={rule['conditions']}"
    #     )

    @staticmethod
    def duplicate_conditions_for_all_targets(conditions: ConditionInterface, targets: List[VariableMetadata]) -> dict:
        """
        Given a list of conditions duplicates the condition for all targets as necessary
        """
        conditions_dict = conditions.get_conditions()
        new_conditions_dict = {}
        for key, conditions_list in conditions_dict.items():
            new_conditions_list = []
            for condition in conditions_list:
                if condition.should_copy():
                    new_conditions_list.extend([condition.copy().set_target(target.name) for target in targets])
                else:
                    new_conditions_list.append(condition)
            new_conditions_dict[key] = new_conditions_list
        return new_conditions_dict

    @classmethod
    def _replace_regex_capture_references(cls, value, captures: dict[str, str]):
        if isinstance(value, str):

            def replace_capture(match):
                capture_name = match.group("name")
                capture_value = captures.get(capture_name)
                if capture_value is None:
                    raise ValueError(f"Unresolved regex capture '{capture_name}'")
                return capture_value

            return cls.REGEX_CAPTURE_REFERENCE.sub(replace_capture, value)
        if isinstance(value, dict):
            return {key: cls._replace_regex_capture_references(item, captures) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._replace_regex_capture_references(item, captures) for item in value]
        if isinstance(value, tuple):
            return tuple(cls._replace_regex_capture_references(item, captures) for item in value)
        return value

    @staticmethod
    def _compile_variable_regex_patterns(patterns: List[str]) -> dict[str, re.Pattern]:
        compiled_patterns = {pattern: re.compile(pattern) for pattern in patterns}
        capture_patterns = {}
        for pattern, compiled_pattern in compiled_patterns.items():
            for capture_name in compiled_pattern.groupindex:
                if capture_name in capture_patterns:
                    raise ValueError(f"Regex capture name '{capture_name}' is defined by multiple patterns")
                capture_patterns[capture_name] = pattern
        return compiled_patterns

    @staticmethod
    def _find_variable_regex_matches(compiled_patterns, targets: List[VariableMetadata]):
        return [
            [(target.name, match) for target in targets if (match := pattern.fullmatch(target.name))]
            for pattern in compiled_patterns.values()
        ]

    @classmethod
    def expand_rule_for_variable_regex(cls, rule: dict, targets: List[VariableMetadata]) -> List[dict]:
        conditions: ConditionInterface = rule["conditions"]
        patterns = list(
            dict.fromkeys(
                condition["value"].get("target")
                for condition in conditions.values()
                if condition.get("value", {}).get("variable_regex_pattern", False)
            )
        )
        if not patterns:
            return [rule]

        compiled_patterns = cls._compile_variable_regex_patterns(patterns)
        matches_by_pattern = cls._find_variable_regex_matches(compiled_patterns, targets)
        if any(not matches for matches in matches_by_pattern):
            return [rule]

        expanded_rules = []
        for matched_values in product(*matches_by_pattern):
            bindings = dict(zip(patterns, (target_name for target_name, _ in matched_values)))
            captures = {
                capture_name: capture_value
                for _, match in matched_values
                for capture_name, capture_value in match.groupdict().items()
            }
            expanded_rule = deepcopy(rule)
            for condition in expanded_rule["conditions"].values():
                value = condition.get("value", {})
                if value.get("variable_regex_pattern", False):
                    value["target"] = bindings[value["target"]]
                    value.pop("variable_regex_pattern")
                condition.update(cls._replace_regex_capture_references(condition, captures))

            for key, value in expanded_rule.items():
                if key != "conditions":
                    expanded_rule[key] = cls._replace_regex_capture_references(value, captures)

            output_variables = []
            for output_variable in expanded_rule.get("output_variables") or []:
                matched_output_variables = [
                    target_name for target_name, _ in matched_values if re.fullmatch(output_variable, target_name)
                ]
                output_variables.extend(matched_output_variables or [output_variable])
            if output_variables:
                expanded_rule["output_variables"] = list(dict.fromkeys(output_variables))
            expanded_rules.append(expanded_rule)

        return expanded_rules

    # @staticmethod
    # def extract_referenced_variables_from_rule(rule: dict):
    #     """
    #     Extracts a list of all variables referenced in a rule.
    #     """
    #     target_names: List[str] = []
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         target = condition["value"].get("target")
    #         comparator = condition["value"].get("comparator")
    #         if target:
    #             target_names.append(target)
    #         if comparator:
    #             target_names.append(comparator)
    #     return target_names

    # @staticmethod
    # def extract_message_from_rule(rule: dict) -> str:
    #     """
    #     Extracts message from rule.
    #     """
    #     actions: List[dict] = rule["actions"]
    #     return actions[0]["params"]["message"]

    @staticmethod
    def valid_rule_structure(rule) -> bool:
        required_keys = ["standards", "core_id"]
        for key in required_keys:
            if key not in rule:
                return False
        return True

    @staticmethod
    def extract_operators_from_conditions(conditions) -> List[str]:
        """
        Extracts all unique operators from rule conditions.
        Handles nested conditions recursively.
        """
        operators = set()

        if not conditions:
            return []
        for key, condition_list in conditions.items():
            if isinstance(condition_list, list):
                for condition in condition_list:
                    if isinstance(condition, dict):
                        operator = condition.get("operator")
                        if operator:
                            operators.add(operator)
                        # Handle nested conditions
                        for nested_key in ["all", "any", "not"]:
                            if nested_key in condition:
                                nested_operators = SQLRuleProcessor.extract_operators_from_conditions(
                                    {nested_key: condition[nested_key]}
                                )
                                operators.update(nested_operators)
                    elif hasattr(condition, "get_conditions"):
                        # Recursive call for ConditionInterface objects
                        nested_operators = SQLRuleProcessor.extract_operators_from_conditions(condition)
                        operators.update(nested_operators)

        return sorted(list(operators))
