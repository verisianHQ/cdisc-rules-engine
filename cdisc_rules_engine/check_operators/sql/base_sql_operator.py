import hashlib
import traceback
from abc import abstractmethod
from functools import wraps
from typing import Any, Dict, Optional, Union

from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    ColumnNotFoundError,
    SqlOperatorError,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata

CHECK_OPERATOR_TABLE_ALIAS = "co"


def log_operator_execution(operator_name):
    """Decorator that takes an operator name parameter."""

    def decorator(func):
        @wraps(func)
        def wrapper(self, other_value, *args, **kwargs):
            try:
                return func(self, other_value)
            except Exception as e:
                logger.error(f"Error in {operator_name}: {str(e)}, traceback: {traceback.format_exc()}")
                raise SqlOperatorError(original_exception=e, operator_name=operator_name) from e

        return wrapper

    return decorator


class BaseSqlOperator:
    def __init__(self, data):
        self.original_data = data
        self.table_id: str = data["dataset_id"]
        self.sql_data_service: PostgresQLDataService = data["data_service"]
        self.column_prefix_map = data.get("column_prefix_map", {})
        self.value_level_metadata = data.get("value_level_metadata", [])
        self.column_codelist_map = data.get("column_codelist_map", {})
        self.codelist_term_maps = data.get("codelist_term_maps", [])
        self.operation_variables: dict[str, SqlOperationResult] = data.get("operation_variables", {})
        self.dataset_metadata: BaseDatasetMetadata = data.get("dataset_metadata", None)

    @abstractmethod
    def execute_operator(self, other_value: Dict[str, Any]):
        pass

    def replace_prefix(self, value: str) -> Union[str, Any]:
        if isinstance(value, str):
            for prefix, replacement in self.column_prefix_map.items():
                if value.startswith(prefix) and replacement is not None:
                    return value.replace(prefix, replacement, 1)
        return value

    def _exists(self, column: str) -> bool:
        if isinstance(column, str) and column in self.operation_variables:
            var = self.operation_variables[column]
            if var.type == "window":
                return self.sql_data_service.pgi.schema.column_exists(self.table_id, var.params.get("column_name"))
            return True
        return self.sql_data_service.pgi.schema.column_exists(self.table_id, column)

    def _do_check_operator(self, sql_subquery_fn) -> str:
        """
        Returns the raw SQL subquery string representing the boolean condition,
        so the SqlRuleCompiler can embed it directly into the WHERE clause.
        """
        subquery = sql_subquery_fn()
        return f"({subquery})"

    def _do_complex_check_operator(self, new_column: str, sql_full_query_fn) -> str:
        """
        Executes an UPDATE query to cache complex calculations into a boolean column natively.
        Returns the SQL column reference (e.g., 'co.my_cached_column') for the WHERE clause.
        """
        new_column = self._resolve_operation_variables_in_cache_key(new_column)

        exists = self.sql_data_service.pgi.schema.column_exists(self.table_id, new_column)
        if not exists:
            self.sql_data_service.pgi.add_column(table=self.table_id, schema=SqlColumnSchema.check_operator(new_column))
            query = sql_full_query_fn(self._table_sql(), self._column_sql(new_column, alias=False))
            self.sql_data_service.pgi.execute_sql(query)

        col_hash = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, new_column)
        return f"{CHECK_OPERATOR_TABLE_ALIAS}.{col_hash}"

    def _get_cache_key_component(self, value) -> str:
        if isinstance(value, str) and value in self.operation_variables:
            op_var = self.operation_variables[value]
            if op_var.type == "window":
                return op_var.params.get("column_name", value)
            resolved_query = op_var.query
            query_hash = hashlib.md5(resolved_query.encode()).hexdigest()[:8]
            return f"op_{query_hash}"
        return str(value)

    def _resolve_operation_variables_in_cache_key(self, cache_key: str) -> str:
        result = cache_key
        for var_name in self.operation_variables:
            if var_name in result:
                var_hash = self._get_cache_key_component(var_name)
                result = result.replace(var_name, var_hash)
        return result

    def _table_sql(self):
        return self.sql_data_service.pgi.schema.get_table_hash(self.table_id)

    def _get_dataset_name_sql(self, lowercase: bool, prefix: Optional[int], suffix: Optional[int]) -> str:
        dataset_name = self.dataset_metadata.name
        if prefix is not None:
            dataset_name = dataset_name[: int(prefix)]
        elif suffix is not None:
            dataset_name = dataset_name[-int(suffix) :] if int(suffix) > 0 else ""
        return self._constant_sql(dataset_name, lowercase=lowercase)

    def _apply_sql_modifiers(self, query: str, lowercase: bool, prefix: Optional[int], suffix: Optional[int]) -> str:
        if lowercase:
            query = f"LOWER({query})"
        if prefix is not None:
            query = f"LEFT({query}, {prefix})"
        if suffix is not None:
            query = f"RIGHT({query}, {suffix})"
        return query

    def _column_sql(
        self,
        column: str,
        lowercase: bool = False,
        prefix: Optional[int] = None,
        suffix: Optional[int] = None,
        alias: bool = True,
        null_return: bool = False,
    ) -> str:

        if isinstance(column, str) and column in self.operation_variables:
            variable = self.operation_variables[column]
            if variable.type == "window":
                column = variable.params.get("column_name")

        if column == DATASET_NAME:
            return self._get_dataset_name_sql(lowercase, prefix, suffix)

        if not self._exists(column):
            if null_return:
                return "NULL"
            raise ColumnNotFoundError(
                column_name=column,
                table_id=self.table_id,
                message=f"Column '{column}' not found in table '{self.table_id}'",
            )

        query = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, column)
        query = f"{CHECK_OPERATOR_TABLE_ALIAS}.{query}" if alias else query

        if query is None:
            raise KeyError(column)

        return self._apply_sql_modifiers(query, lowercase, prefix, suffix)

    def _constant_sql(self, value: Any, lowercase: bool = False) -> str:
        if isinstance(value, str):
            return self._handle_string_constant(value, lowercase)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif value is None:
            return "NULL"
        else:
            raise ValueError(f"Unsupported constant type: {type(value)}")

    def _handle_string_constant(self, value: str, lowercase: bool) -> str:
        if value in self.operation_variables:
            variable = self.operation_variables[value]
            if variable.type in ("window", "column"):
                return self._column_sql(variable.params.get("column_name"), lowercase=lowercase)
            query = self._process_constant_operation_variable(value)
        else:
            query = f"'{value.replace(chr(39), chr(39) + chr(39))}'"

        if lowercase:
            query = f"LOWER({query})"
        return query

    def _process_constant_operation_variable(self, value: str) -> str:
        variable = self.operation_variables[value]
        if variable.type != "constant":
            raise ValueError(f"Variable {value} is not a constant.")

        query = variable.query
        if variable.params:
            query = self._substitute_operation_parameters(query, variable.params)
        return f"({query})"

    def _substitute_operation_parameters(self, query: str, params: dict) -> str:
        for param_placeholder, column_name in params.items():
            column_sql = self._column_sql(column_name)
            query = query.replace(param_placeholder, column_sql)
        return query

    def _collection_sql(self, value: Any, lowercase: bool = False) -> str:
        if isinstance(value, list):
            return f"({', '.join(self._constant_sql(v, lowercase=lowercase) for v in value)})"
        elif isinstance(value, str):
            return self._handle_string_collection(value, lowercase)
        elif value is None:
            return ""
        else:
            raise ValueError(f"Unsupported collection type: {type(value)}")

    def _handle_string_collection(self, value: str, lowercase: bool) -> str:
        variable = self.operation_variables[value]
        query = variable.query
        if variable.params:
            query = self._substitute_operation_parameters(query, variable.params)
        query = f"({query})"
        if lowercase:
            query = f"(SELECT LOWER(value) FROM {query})"
        return query

    def _sql(self, value: Any, lowercase: bool = False, value_is_literal: bool = False) -> str:
        if isinstance(value, str) and not value_is_literal:
            if value in self.operation_variables:
                variable = self.operation_variables[value]
                if variable.type == "window":
                    return self._column_sql(variable.params.get("column_name"), lowercase=lowercase)
                elif variable.type == "constant":
                    return self._constant_sql(value, lowercase=lowercase)
                elif variable.type == "collection":
                    return self._collection_sql(value, lowercase=lowercase)
                else:
                    raise ValueError(f"Unsupported variable type: {variable.type} for variable {value}.")
            elif self.sql_data_service.pgi.schema.column_exists(self.table_id, value):
                return self._column_sql(value, lowercase=lowercase)

        return self._constant_sql(value, lowercase=lowercase)

    def _is_empty_sql(self, target: str, alias: bool = True) -> str:
        if isinstance(target, str):
            if target in self.operation_variables:
                var = self.operation_variables[target]
                if var.type == "window":
                    return self._is_empty_sql_column(var.params.get("column_name"), alias)
                return self._is_empty_sql_operation_variable(target)
            elif self.sql_data_service.pgi.schema.get_column(self.table_id, target) is not None:
                return self._is_empty_sql_column(target, alias)
            else:
                return "FALSE"

        if isinstance(target, str) and target == "":
            return "TRUE"
        elif target is None:
            return "TRUE"
        else:
            return "FALSE"

    def _is_empty_sql_column(self, col: str, alias: bool = True) -> str:
        column = self.sql_data_service.pgi.schema.get_column(self.table_id, col)
        if not column:
            raise ColumnNotFoundError(col, self.table_id)

        key = column.hash
        if alias:
            key = f"{CHECK_OPERATOR_TABLE_ALIAS}.{key}"

        match column.type:
            case "Char":
                return f"({key} IS NULL OR {key} = '')"
            case "Bool":
                return f"({key} IS NULL)"
            case "Num":
                return f"({key} IS NULL)"
            case "Date":
                return f"({key} IS NULL)"
            case _:
                raise ValueError(f"Unsupported column type: {column.type} for column {col}.")

    def _is_empty_sql_operation_variable(self, target: str) -> str:
        variable = self.operation_variables[target]
        if variable.type != "constant":
            return f"(NOT EXISTS (SELECT 1 FROM ({variable.query}) AS op))"

        query = variable.query
        if variable.params:
            for param_placeholder, column_name in variable.params.items():
                column_sql = self._column_sql(column_name)
                query = query.replace(param_placeholder, column_sql)

        match variable.subtype:
            case "Char":
                return f"(({query}) IS NULL OR ({query}) = '')"
            case "Bool":
                return f"(({query}) IS NULL)"
            case "Num":
                return f"(({query}) IS NULL)"
            case "Date":
                return f"(({query}) IS NULL)"
            case _:
                raise ValueError(f"Unsupported variable type: {variable.subtype} for variable {target}.")

    def _filter_params(self, other_value, ex_dict_table_name):
        filter_attribute = other_value.get("filter_attribute")
        filter_value = other_value.get("filter_value")

        if filter_attribute and filter_value:
            if not self.sql_data_service.pgi.schema.column_exists(ex_dict_table_name, filter_attribute):
                raise ValueError(f"Filter attribute '{filter_attribute}' is not a column in {ex_dict_table_name}.")

            if filter_value in self.operation_variables:
                attribute_op_result = self.operation_variables[filter_value]
                self.sql_data_service.pgi.execute_sql(attribute_op_result.query)
                filter_value = self.sql_data_service.pgi.fetch_one()["value"]

            filter_value = filter_value.replace("'", "").replace('"', "").strip()

        return filter_attribute, filter_value
