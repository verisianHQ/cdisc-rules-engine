import re
import traceback
from copy import deepcopy
from typing import List, Union

from business_rules import export_rule_data
from psycopg2.errors import ProgrammingError

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundError,
    DomainNotFoundInDefineXMLError,
    FailedSchemaValidation,
    RuleFormatError,
    VariableMetadataNotFoundError,
    ColumnNotFoundError,
    SqlOperatorError,
)
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import (
    ConditionCompositeFactory,
)
from cdisc_rules_engine.models.sql_venmo_object import SqlVenmoObject
from cdisc_rules_engine.models.sql_venmo_result_handler import (
    SqlVenmoResultHandler,
)
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_dataset_builders import sql_builder_factory
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlOperationError
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.utilities.sql_rule_compiler import SqlRuleCompiler
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor


def clean_postgres_message(message: str) -> str:
    """Clean non-deterministic details from PostgreSQL error messages."""
    if "does not exist" in message:
        return "column or relation does not exist"
    elif "syntax error" in message:
        return "SQL syntax error"
    elif "invalid input syntax" in message:
        return "invalid input syntax"
    return message


class SQLRulesEngine:
    def __init__(
        self,
        data_service: PostgresQLDataService,
        standards_context: BaseStandardsContext,
    ):
        self.data_service = data_service
        self.standards_context = standards_context

    def get_schema(self):
        return export_rule_data(SqlVenmoObject, SqlVenmoResultHandler)

    def sql_validate_single_rule(self, rule: dict):
        results = {}
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        is_study_sensitivity = rule.get("sensitivity") == Sensitivity.STUDY.value
        study_error_already_reported = False

        all_datasets = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        for pp_ds_id in self.data_service.get_uploaded_dataset_ids():
            dataset_metadata = self.data_service.get_dataset_metadata(pp_ds_id)

            is_suitable, reason = self.standards_context.within_rule_scope(
                rule,
                next(
                    (metadata for metadata in self.data_service.datasets if metadata.name.lower() == pp_ds_id.lower()),
                    None,
                ),
            )
            if is_suitable:
                if is_study_sensitivity and study_error_already_reported:
                    results[dataset_metadata.name] = [
                        ValidationErrorContainer(
                            **{
                                "dataset": dataset_metadata.filename,
                                "domain": dataset_metadata.domain,
                                "errors": [],
                            }
                        ).to_representation()
                    ]
                    continue

                dataset_results = self.validate_single_dataset(rule, dataset_metadata, all_datasets)
                results[dataset_metadata.name] = dataset_results
                if is_study_sensitivity and self._contains_error_entries(dataset_results):
                    study_error_already_reported = True
            else:
                logger.info(f"Skipped dataset {dataset_metadata.name}. Reason: {reason}")
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    message=reason,
                    dataset=dataset_metadata.filename,
                    domain=dataset_metadata.domain,
                )
                results[pp_ds_id] = [error_obj.to_representation()]
        return results

    @staticmethod
    def _contains_error_entries(result_entries: List[Union[dict, str]]) -> bool:
        for entry in result_entries:
            if isinstance(entry, dict) and entry.get("errors"):
                return True
        return False

    def validate_single_dataset(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        datasets: List[BaseDatasetMetadata],
    ) -> List[Union[dict, str]]:
        logger.info(f"Validating {dataset_metadata.name}. rule={rule}.")
        try:
            result: List[Union[dict, str]] = self.validate_rule(rule, dataset_metadata, datasets)
            logger.info(f"Validated dataset {dataset_metadata.name}. Result = {result}")
            if result:
                return result
            else:
                return [
                    ValidationErrorContainer(
                        **{
                            "dataset": dataset_metadata.filename,
                            "domain": dataset_metadata.domain,
                            "errors": [],
                        }
                    ).to_representation()
                ]
        except Exception as e:
            logger.trace(e)
            logger.error(
                f"""Error occurred during validation.
            Error: {e}
            Error Type: {type(e)}
            Error Message: {str(e)}
            Full traceback:
            {traceback.format_exc()}
            """
            )
            error_obj: ValidationErrorContainer = self.handle_validation_exceptions(e, dataset_metadata.filename)
            error_obj.domain = dataset_metadata.domain
            return [error_obj.to_representation()]

    def validate_rule(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        datasets: List[BaseDatasetMetadata],
    ) -> List[Union[dict, str]]:
        builder = sql_builder_factory.get_service(
            rule_type=rule.get("rule_type"),
            rule=rule,
            data_service=self.data_service,
            dataset_metadata=dataset_metadata,
            datasets=datasets,
            standards_context=self.standards_context,
        )

        dataset_id, base_query = builder.get_dataset_query()
        return self.execute_rule(rule, dataset_metadata, dataset_id, base_query)

    def execute_rule(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        dataset_id: str,
        base_query: str,
    ) -> List[str]:
        rule_copy = deepcopy(rule)
        updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"],
            dataset_metadata.variables,
        )
        rule_copy["conditions"].set_conditions(updated_conditions)

        operation_variables, current_dataset_id, operations_query = SQLRuleProcessor.perform_rule_operations(
            rule_copy,
            dataset_id,
            dataset_metadata,
            data_service=self.data_service,
            standards_context=self.standards_context,
        )

        compiler = SqlRuleCompiler(self.data_service, current_dataset_id, dataset_metadata, operation_variables)
        where_clause = compiler.compile(rule_copy["conditions"].to_dict())

        results = []
        handler = SqlVenmoResultHandler(
            results,
            dataset_metadata=dataset_metadata,
            rule=rule,
            data_service=self.data_service,
            dataset_id=current_dataset_id,
            operation_variables=operation_variables,
        )

        handler.evaluate_sql(where_clause, operations_query)

        return results

    def handle_validation_exceptions(self, exception, name) -> ValidationErrorContainer:  # noqa
        if isinstance(exception, DatasetNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Dataset Not Found",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, RuleFormatError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Rule format error",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, AssertionError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Rule format error",
                message="Rule contains invalid operator",
            )
            message = "rule execution error"
        elif isinstance(exception, ColumnNotFoundError):
            error_obj: ValidationErrorContainer = ValidationErrorContainer(
                status=ExecutionStatus.SKIPPED.value,
                dataset=name,
            )
            message = exception.message
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=name,
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        elif isinstance(exception, KeyError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Column not found in data",
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, DomainNotFoundInDefineXMLError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error=DomainNotFoundInDefineXMLError.description,
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, VariableMetadataNotFoundError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error=VariableMetadataNotFoundError.description,
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, FailedSchemaValidation):
            if self.validate_xml:
                message = ", ".join(sorted(exception.args[0].split(", ")))
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    error=FailedSchemaValidation.description,
                    message=message,
                )
                message = "Schema Validation Error"
                errors = [error_obj]
                return ValidationErrorContainer(
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SUCCESS.value,
                    dataset=name,
                )
            else:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    dataset=name,
                )
                message = "Skipped because schema validation is off"
                errors = [error_obj]
                return ValidationErrorContainer(
                    dataset=name,
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SKIPPED.value,
                )
        elif isinstance(exception, DomainNotFoundError):
            error_obj = ValidationErrorContainer(
                dataset=name,
                message=str(exception),
                status=ExecutionStatus.SKIPPED.value,
            )
            message = "rule evaluation skipped - operation domain not found"
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=name,
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        elif isinstance(exception, SqlOperatorError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error=f"SQL error in {exception.operator_name} operator",
                message=clean_postgres_message(str(exception.original_exception)),
            )
            message = "SQL operator execution error"
        elif isinstance(exception, SqlOperationError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error=f"SQL error in {exception.operation_name} operation",
                message=clean_postgres_message(str(exception.original_exception)),
            )
            message = "SQL operation execution error"
        elif isinstance(exception, ProgrammingError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="PostgreSQL Error",
                message=clean_postgres_message(str(exception)),
            )
            message = "SQL execution error"
        elif isinstance(exception, ValueError):
            error_message = str(exception)
            schema_pattern = r"Column\s+(\w+)\s+or\s+(\w+)\s+not found in the respective schemas"
            match = re.search(schema_pattern, error_message, re.IGNORECASE)

            if match:
                column_name = match.group(1).upper()

                error_obj = FailedValidationEntity(
                    dataset=name,
                    error="Column not found in data",
                    message=column_name,
                )
                message = "rule execution error"
            else:
                column_pattern = r"Column\s+['\"]?(\w+)['\"]?\s+(?:does not exist|not found|missing)"
                match = re.search(column_pattern, error_message, re.IGNORECASE)

                if match:
                    column_name = match.group(1).upper()
                    error_obj = FailedValidationEntity(
                        dataset=name,
                        error="Column not found in data",
                        message=column_name,
                    )
                    message = "rule execution error"
                else:
                    error_obj = FailedValidationEntity(
                        dataset=name,
                        error="Validation error",
                        message=error_message,
                    )
                    message = "rule execution error"
        else:
            error_obj = FailedValidationEntity(
                dataset=name,
                error="An unknown exception has occurred",
                message=str(exception),
            )
            message = "rule execution error"
        errors = [error_obj]
        return ValidationErrorContainer(
            dataset=name,
            errors=errors,
            message=message,
            status=ExecutionStatus.EXECUTION_ERROR.value,
        )
