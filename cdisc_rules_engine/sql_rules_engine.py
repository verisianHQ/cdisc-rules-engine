from copy import deepcopy
from typing import Iterable, List, Union

from business_rules import export_rule_data
from business_rules.engine import run
import os
from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundInDefineXMLError,
    RuleFormatError,
    VariableMetadataNotFoundError,
    FailedSchemaValidation,
    DomainNotFoundError,
)
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    ConfigInterface,
)
from cdisc_rules_engine.interfaces.PostgresQLDataService import PostgresQLDataService
from cdisc_rules_engine.interfaces.SQLDataService import SQLDataService
from cdisc_rules_engine.models.actions import COREActions
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import (
    ConditionCompositeFactory,
)
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.utilities.sql_data_processor import SQLDataProcessor
from cdisc_rules_engine.utilities.sql_dataset_preprocessor import SQLDatasetPreprocessor
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from cdisc_rules_engine.utilities.utils import (
    serialize_rule,
)
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
import traceback


class SQLRulesEngine:
    def __init__(
        self,
        cache: CacheServiceInterface,
        data_service: SQLDataService,
        config_obj: ConfigInterface = None,
        external_dictionaries: ExternalDictionariesContainer = ExternalDictionariesContainer(),
        **kwargs,
    ):
        self.cache = cache or CacheServiceFactory(self.config).get_cache_service()
        self.data_service = data_service

        self.config = config_obj or default_config
        self.standard = kwargs.get("standard")
        self.standard_version = (kwargs.get("standard_version") or "").replace(".", "-")
        self.standard_substandard = kwargs.get("standard_substandard") or None

        # TODO: remove eventually
        self.dataset_implementation = PandasDataset
        kwargs["dataset_implementation"] = self.dataset_implementation

        # TODO: move into data service
        self.max_dataset_size = kwargs.get("max_dataset_size")
        self.dataset_paths = kwargs.get("dataset_paths")
        self.ct_packages = kwargs.get("ct_packages", [])
        self.ct_package = kwargs.get("ct_package")
        self.external_dictionaries = external_dictionaries
        self.define_xml_path: str = kwargs.get("define_xml_path")
        self.validate_xml: bool = kwargs.get("validate_xml")
        self.data_processor = SQLDataProcessor(self.data_service, self.cache)

        # this stays
        self.rule_processor = SQLRuleProcessor(self.data_service, self.cache)

    def get_schema(self):
        return export_rule_data(DatasetVariable, COREActions)

    # needs to take the dataservice, then go through the dataset metadata to answer below question
    def sql_validate_single_rule(self, rule: dict, ds: PostgresQLDataService):
        results = {}
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        for dataset_id in ds.data_dfs().keys():
            cur_domain = ds.get_domain(dataset_id)
            if "domains" in rule and cur_domain in results:
                include_split = rule["domains"].get("include_split_datasets", False)
                if not include_split:
                    continue  # handling split datasets
            else:
                results[cur_domain] = self.validate_single_dataset(rule, ds, dataset_id)
        return results

    def validate_single_dataset(self, rule: dict, ds: PostgresQLDataService, dataset_id: str) -> List[Union[dict, str]]:
        """
        This function is an entrypoint to validation process.
        It validates a given rule against datasets.
        """
        dataset_path = ds.get_full_path(dataset_id)
        dataset_domain = ds.get_domain(dataset_id)
        dataset_rdomain = ds.get_rdomain(dataset_id)
        dataset_filename = ds.get_filename(dataset_id)
        logger.info(f"Validating {dataset_id}. " f"rule={rule}. dataset_path={dataset_path}.")
        try:
            is_suitable, reason = self.rule_processor.is_suitable_for_validation(
                rule,
                ds,
                dataset_id,
                self.standard,
                self.standard_substandard,
            )
            if is_suitable:
                # TODO: continue here
                result: List[Union[dict, str]] = self.validate_rule(rule, ds, dataset_id)
                logger.info(f"Validated dataset {dataset_id}. Result = {result}")
                if result:
                    return result
                else:
                    # No errors were generated, create success error container
                    return [
                        ValidationErrorContainer(
                            **{
                                "dataset": dataset_filename,
                                "domain": dataset_domain or dataset_rdomain,
                                "errors": [],
                            }
                        ).to_representation()
                    ]
            else:
                logger.info(f"Skipped dataset {dataset_id}. Reason: {reason}")
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    message=reason,
                    dataset=dataset_filename,
                    domain=dataset_domain or dataset_rdomain or "",
                )
                return [error_obj.to_representation()]
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
            error_obj: ValidationErrorContainer = self.handle_validation_exceptions(e, dataset_path, dataset_path)
            error_obj.domain = dataset_domain or dataset_rdomain or ""
            # this wrapping into a list is necessary to keep return type consistent
            return [error_obj.to_representation()]

    # def get_dataset_builder(
    #     self,
    #     rule: dict,
    #     datasets: Iterable[SDTMDatasetMetadata],
    #     dataset_metadata: SDTMDatasetMetadata,
    # ):
    #     return builder_factory.get_service(
    #         rule.get("rule_type"),
    #         rule=rule,
    #         data_service=self.data_service,
    #         cache_service=self.cache,
    #         data_processor=self.data_processor,
    #         rule_processor=self.rule_processor,
    #         dataset_metadata=dataset_metadata,
    #         datasets=datasets,
    #         dataset_path=dataset_metadata.full_path,
    #         define_xml_path=self.define_xml_path,
    #         standard=self.standard,
    #         standard_version=self.standard_version,
    #         standard_substandard=self.standard_substandard,
    #         library_metadata=self.library_metadata,
    #         dataset_implementation=self.data_service.dataset_implementation,
    #     )

    def validate_rule(
        self,
        rule: dict,
        ds: PostgresQLDataService,
        dataset_id: str,
    ) -> List[Union[dict, str]]:
        """
         This function is an entrypoint for rule validation.
        It defines a rule validator based on its type and calls it.
        """
        kwargs = {}
        # builder = self.get_dataset_builder(rule, datasets, dataset_metadata)
        # dataset = builder.get_dataset()

        # TODO: wrong logic, this should be dependent on the rule not whether the library metadata is present
        # if self.library_metadata:
        #     kwargs["variable_codelist_map"] = self.library_metadata.variable_codelist_map
        #     kwargs["codelist_term_maps"] = self.library_metadata.get_all_ct_package_metadata()

        # Update rule for certain rule types
        # SPECIAL CASES FOR RULE TYPES ###############################
        # TODO: Handle these special cases better.
        if rule.get("rule_type") == RuleTypes.DEFINE_ITEM_METADATA_CHECK.value:
            kwargs["variable_codelist_map"] = (
                ds.get_variable_codelist_map()
            )  # self.library_metadata.variable_codelist_map
            kwargs["codelist_term_maps"] = (
                ds.get_all_ct_package_metadata()
            )  # self.library_metadata.get_all_ct_package_metadata()
        elif (
            rule.get("rule_type") == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value
            or rule.get("rule_type") == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value
        ):
            self.rule_processor.add_comparator_to_rule_conditions(rule, comparator=None, target_prefix="define_")
        elif rule.get("rule_type") == RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value:
            kwargs["value_level_metadata"] = ds.get_define_xml_value_level_metadata()
            # self.get_define_xml_value_level_metadata(ds.get_full_path(dataset_id), ds.get_unsplit_name(dataset_id))

        elif rule.get("rule_type") == RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value:
            ig_variable_metadata = ds.get_ig_variables_metadata(dataset_id)
            # library_metadata: dict = self.library_metadata.variables_metadata.get(dataset_metadata.domain, {})
            define_metadata = ds.get_define_xml_variables_metadata(dataset_id)
            # define_metadata: List[dict] = builder.get_define_xml_variables_metadata()
            dataset_variables = ds.get_dataset_variables(dataset_id)
            targets: List[str] = self.data_processor.filter_dataset_columns_by_metadata_and_rule(
                dataset_variables, define_metadata, ig_variable_metadata, rule
            )
            rule_copy = deepcopy(rule)
            updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(rule_copy["conditions"], targets)
            rule_copy["conditions"].set_conditions(updated_conditions)
            # When duplicating conditions,
            # rule should be copied to prevent updates to concurrent rule executions
            return self.execute_rule(rule_copy, ds, dataset_id, **kwargs)

        kwargs["ct_packages"] = list(self.ct_packages)

        # logger.info(f"Using dataset build by: {builder.__class__}")
        return self.execute_rule(rule, ds, dataset_id, **kwargs)

    def execute_rule(
        self,
        rule: dict,
        ds: PostgresQLDataService,
        dataset_id: str,
        # dataset: DatasetInterface,
        # datasets: Iterable[SDTMDatasetMetadata],
        # dataset_metadata: SDTMDatasetMetadata,
        value_level_metadata: List[dict] = None,
        variable_codelist_map: dict = None,
        codelist_term_maps: list = None,
        ct_packages: list = None,
    ) -> List[str]:
        """
        Executes the given rule on a given dataset.
        """
        if value_level_metadata is None:
            value_level_metadata = []
        if variable_codelist_map is None:
            variable_codelist_map = {}
        if codelist_term_maps is None:
            codelist_term_maps = []
        # Add conditions to rule for all variables if variables: all appears
        # in condition
        rule_copy = deepcopy(rule)
        updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"], ds.get_dataset_variables(dataset_id)
        )
        rule_copy["conditions"].set_conditions(updated_conditions)
        # Adding copy for now to avoid updating cached dataset
        # dataset = deepcopy(dataset)
        # preprocess dataset
        dataset_preprocessor = SQLDatasetPreprocessor(ds, dataset_id, self.cache)
        dataset = dataset_preprocessor.preprocess(rule_copy, datasets)
        dataset = self.rule_processor.perform_rule_operations(
            rule_copy,
            dataset,
            ds.get_unsplit_name(dataset_id),
            datasets,
            ds.get_full_path(dataset_id),
            standard=self.standard,
            standard_version=self.standard_version,
            standard_substandard=self.standard_substandard,
            external_dictionaries=self.external_dictionaries,
            ct_packages=ct_packages,
        )
        dataset_variable = DatasetVariable(
            dataset,
            column_prefix_map={"--": dataset_metadata.domain},
            value_level_metadata=value_level_metadata,
            column_codelist_map=variable_codelist_map,
            codelist_term_maps=codelist_term_maps,
        )
        results = []
        run(
            serialize_rule(rule_copy),  # engine expects a JSON serialized dict
            defined_variables=dataset_variable,
            defined_actions=COREActions(
                results,
                variable=dataset_variable,
                dataset_metadata=dataset_metadata,
                rule=rule,
                value_level_metadata=value_level_metadata,
            ),
        )
        return results

    def get_define_xml_value_level_metadata(self, dataset_path: str, domain_name: str) -> List[dict]:
        """
        Gets Define XML variable metadata and returns it as dataframe.
        """
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_value_level_metadata(domain_name=domain_name)

    def handle_validation_exceptions(self, exception, dataset_path, file_name) -> ValidationErrorContainer:  # noqa
        if isinstance(exception, DatasetNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Dataset Not Found",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, RuleFormatError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Rule format error",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, AssertionError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Rule format error",
                message="Rule contains invalid operator",
            )
            message = "rule execution error"
        elif isinstance(exception, KeyError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Column not found in data",
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, DomainNotFoundInDefineXMLError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error=DomainNotFoundInDefineXMLError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, VariableMetadataNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error=VariableMetadataNotFoundError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, FailedSchemaValidation):
            if self.validate_xml:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    error=FailedSchemaValidation.description,
                    message=exception.args[0],
                )
                message = "Schema Validation Error"
                errors = [error_obj]
                return ValidationErrorContainer(
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SUCCESS.value,
                    dataset=os.path.basename(dataset_path),
                )
            else:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    dataset=os.path.basename(dataset_path),
                )
                message = "Skipped because schema validation is off"
                errors = [error_obj]
                return ValidationErrorContainer(
                    dataset=os.path.basename(dataset_path),
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SKIPPED.value,
                )
        elif isinstance(exception, DomainNotFoundError):
            error_obj = ValidationErrorContainer(
                dataset=os.path.basename(dataset_path),
                message=str(exception),
                status=ExecutionStatus.SKIPPED.value,
            )
            message = "rule evaluation skipped - operation domain not found"
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=os.path.basename(dataset_path),
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        else:
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="An unknown exception has occurred",
                message=str(exception),
            )
            message = "rule execution error"
        errors = [error_obj]
        return ValidationErrorContainer(
            dataset=os.path.basename(dataset_path),
            errors=errors,
            message=message,
            status=ExecutionStatus.EXECUTION_ERROR.value,
        )
