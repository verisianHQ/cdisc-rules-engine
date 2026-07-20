from typing import List, Tuple

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.dataset_metadata2 import VariableMetadata
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


class SQLRuleProcessor:

    @classmethod
    def perform_rule_operations(
        cls,
        rule: dict,
        dataset_id: str,
        dataset_metadata: BaseDatasetMetadata,
        data_service: PostgresQLDataService,
        standards_context: BaseStandardsContext,
    ) -> Tuple[dict[str, SqlOperationResult], str, str]:
        operations: List[dict] = rule.get("operations") or []
        output_variables: dict[str, SqlOperationResult] = {}
        window_ops = {}

        for operation in operations:
            output_variable, op_result = cls._execute_single_operation(
                operation,
                rule,
                dataset_id,
                dataset_metadata,
                data_service,
                standards_context,
                output_variables,
            )
            output_variables[output_variable] = op_result
            if op_result.type == "window":
                window_ops[output_variable] = op_result

        if window_ops:
            return cls._materialise_window_operations(
                window_ops,
                rule,
                dataset_id,
                output_variables,
                data_service,
            )

        original_table_hash = data_service.pgi.schema.get_table_hash(dataset_id)
        operations_query = f"SELECT * FROM {original_table_hash} t"
        return output_variables, dataset_id, operations_query

    @classmethod
    def _execute_single_operation(
        cls,
        operation: dict,
        rule: dict,
        dataset_id: str,
        dataset_metadata: BaseDatasetMetadata,
        data_service: PostgresQLDataService,
        standards_context: BaseStandardsContext,
        output_variables: dict[str, SqlOperationResult],
    ) -> Tuple[str, SqlOperationResult]:
        rule_name = operation.get("operator", "")
        output_variable = operation.get("id", "")

        if not output_variable.startswith("$"):
            raise ValueError(
                f"Output variable must start with '$', "
                f"but got '{output_variable}' in rule {rule.get('core_id', 'unknown')}"
            )

        params = cls._build_operation_params(
            operation,
            dataset_id,
            dataset_metadata,
            standards_context,
            output_variables,
        )

        operation_obj = SqlOperationsFactory.get_service(rule_name, params=params, data_service=data_service)
        op_result = operation_obj.execute()

        if op_result.type == "window":
            op_result.params = op_result.params or {}
            op_result.params["column_name"] = output_variable.replace("$", "op_").replace("-", "_")

        logger.info(f"Processed rule operation. operation={rule_name}, rule={rule}")
        return output_variable, op_result

    @staticmethod
    def _build_operation_params(
        operation: dict,
        dataset_id: str,
        dataset_metadata: BaseDatasetMetadata,
        standards_context: BaseStandardsContext,
        output_variables: dict[str, SqlOperationResult],
    ) -> SqlOperationParams:
        target_variable: str = operation.get("name", None)
        operation_domain: str = operation.get("domain", dataset_metadata.domain)

        if target_variable:
            target_variable = standards_context.replace_domain_code(dataset_metadata, target_variable)

        return SqlOperationParams(
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
            table=dataset_id,
            use_rule_type_table=operation.get("use_rule_type_table", False),
        )

    @staticmethod
    def _materialise_window_operations(
        window_ops: dict,
        rule: dict,
        dataset_id: str,
        output_variables: dict[str, SqlOperationResult],
        data_service: PostgresQLDataService,
    ) -> Tuple[dict[str, SqlOperationResult], str, str]:
        new_dataset_id = f"{dataset_id}_{rule.get('core_id', 'op')}_ops"

        existing_schema = data_service.pgi.schema.get_table(new_dataset_id)
        if existing_schema is not None:
            operations_query = f"SELECT * FROM {existing_schema.hash} t"
            return output_variables, new_dataset_id, operations_query

        original_schema = data_service.pgi.schema.get_table(dataset_id)
        original_table_hash = original_schema.hash

        new_schema = SqlTableSchema.derived(new_dataset_id, data_service.pgi)
        for _, col in original_schema.get_columns():
            new_schema.add_column(col)

        joins = []
        select_cols = ["t.*"]

        for i, (var_name, op_result) in enumerate(window_ops.items()):
            op_result.params = op_result.params or {}
            if "column_name" not in op_result.params:
                op_result.params["column_name"] = var_name.replace("$", "op_").replace("-", "_")

            clean_name = op_result.params["column_name"]
            new_schema.add_column(SqlColumnSchema.generated(clean_name, op_result.subtype))
            col_hash = new_schema.get_column_hash(clean_name)

            alias = f"op_{i}"
            joins.append(f"LEFT JOIN ({op_result.query}) {alias} ON t.id = {alias}.id")
            select_cols.append(f"{alias}.value AS {col_hash}")

        nested_query = f"""
            SELECT {', '.join(select_cols)}
            FROM {original_table_hash} t
            {' '.join(joins)}
        """

        create_table_query = f"CREATE UNLOGGED TABLE {new_schema.hash} AS {nested_query}"
        data_service.pgi.execute_sql(create_table_query)
        data_service.pgi.schema.add_table(new_schema)

        operations_query = f"SELECT * FROM {new_schema.hash} t"

        return output_variables, new_dataset_id, operations_query

    @staticmethod
    def duplicate_conditions_for_all_targets(conditions: ConditionInterface, targets: List[VariableMetadata]) -> dict:
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

    @staticmethod
    def valid_rule_structure(rule) -> bool:
        required_keys = ["standards", "core_id"]
        for key in required_keys:
            if key not in rule:
                return False
        return True

    @staticmethod
    def extract_operators_from_conditions(conditions) -> List[str]:
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
                        for nested_key in ["all", "any", "not"]:
                            if nested_key in condition:
                                nested_operators = SQLRuleProcessor.extract_operators_from_conditions(
                                    {nested_key: condition[nested_key]}
                                )
                                operators.update(nested_operators)
                    elif hasattr(condition, "get_conditions"):
                        nested_operators = SQLRuleProcessor.extract_operators_from_conditions(condition)
                        operators.update(nested_operators)

        return sorted(list(operators))
