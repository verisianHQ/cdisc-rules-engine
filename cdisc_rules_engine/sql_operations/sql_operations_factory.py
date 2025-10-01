from cdisc_rules_engine.sql_operations.dataset_names import SqlDatasetNamesOperation
from cdisc_rules_engine.sql_operations.distinct import SqlDistinctOperation
from cdisc_rules_engine.sql_operations.domain_label import SqlDomainLabelOperation
from cdisc_rules_engine.sql_operations.day_data_validator import SqlDayDataValidatorOperation
from cdisc_rules_engine.sql_operations.get_model_filtered_variables import SqlGetModelFilteredVariablesOperation
from cdisc_rules_engine.sql_operations.numeric_operation import (
    SqlNumericOperation,
)
from cdisc_rules_engine.sql_operations.date_operation import SqlDateOperation
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.sql_operations.variable_exists import SqlVariableExistsOperation
from cdisc_rules_engine.sql_operations.dataset_column_order import SqlDatasetColumnOrderOperation


class SqlOperationsFactory:
    _operations_map = {
        "codelist_extensible": None,
        "codelist_terms": None,
        "dataset_names": SqlDatasetNamesOperation,
        "define_extensible_codelists": None,
        "distinct": SqlDistinctOperation,
        "dy": SqlDayDataValidatorOperation,
        "extract_metadata": None,
        "get_column_order_from_dataset": lambda params, ds: SqlDatasetColumnOrderOperation(params, ds),
        "get_column_order_from_library": None,
        "get_codelist_attributes": None,
        "get_model_column_order": None,
        "get_model_filtered_variables": SqlGetModelFilteredVariablesOperation,
        "get_parent_model_column_order": None,
        "map": None,
        "max": lambda params, ds: SqlNumericOperation(params, ds, "MAX"),
        "max_date": lambda params, ds: SqlDateOperation(params, ds, "MAX"),
        "mean": lambda params, ds: SqlNumericOperation(params, ds, "AVG"),
        "min": lambda params, ds: SqlNumericOperation(params, ds, "MIN"),
        "min_date": lambda params, ds: SqlDateOperation(params, ds, "MIN"),
        "record_count": lambda params, ds: SqlNumericOperation(params, ds, "COUNT"),
        "valid_meddra_code_references": None,
        "valid_whodrug_references": None,
        "whodrug_code_hierarchy": None,
        "valid_meddra_term_references": None,
        "valid_meddra_code_term_pairs": None,
        "variable_exists": SqlVariableExistsOperation,
        "variable_names": None,
        "variable_library_metadata": None,
        "variable_value_count": None,
        "variable_count": None,
        "variable_is_null": None,
        "domain_is_custom": None,
        "domain_label": lambda params, ds: SqlDomainLabelOperation(params, ds),
        "required_variables": None,
        "expected_variables": None,
        "permissible_variables": None,
        "study_domains": None,
        "valid_codelist_dates": None,
        "label_referenced_variable_metadata": None,
        "name_referenced_variable_metadata": None,
        "define_variable_metadata": None,
        "valid_external_dictionary_value": None,
        "valid_external_dictionary_code": None,
        "valid_external_dictionary_code_term_pair": None,
        "valid_define_external_dictionary_version": None,
        "get_dataset_filtered_variables": None,
    }

    @classmethod
    def get_service(
        cls,
        name: str,
        **kwargs,
    ) -> SqlBaseOperation:
        """Get instance of SQL operation that matches operation specified in params"""
        required_args = {
            "params",
            "data_service",
            "library_metadata",
        }
        if not required_args.issubset(kwargs.keys()):
            raise ValueError(f"One of the following required key word arguments is missing: " f"{required_args}")
        if name in cls._operations_map:
            operation = cls._operations_map.get(name)
            if operation is None:
                raise NotImplementedError(f"Operation {name} is not implemented")

            # Check if operation is a lambda function or a class
            if callable(operation) and hasattr(operation, "__name__") and operation.__name__ == "<lambda>":
                # For lambda functions, call with params and data_service only
                return operation(kwargs.get("params"), kwargs.get("data_service"))
            else:
                # For classes, call with all parameters including library_metadata
                return operation(
                    kwargs.get("params"), kwargs.get("data_service"), library_metadata=kwargs.get("library_metadata")
                )

        raise ValueError(
            f"Operation name must be in  {list(cls._operations_map.keys())}, " f"given operation name is {name}"
        )
