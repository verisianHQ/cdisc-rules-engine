import traceback
from abc import abstractmethod
from functools import wraps
from typing import Any, List, Union

import numpy as np
import pandas as pd

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.services import logger


def log_operator_execution(func):
    @wraps(func)
    def wrapper(self, other_value, *args, **kwargs):
        try:
            logger.info(f"Starting check operator: {func.__name__}")
            result = func(self, other_value)
            logger.info(f"Completed check operator: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}, " f"traceback: {traceback.format_exc()}")
            error_message = str(e)
            if isinstance(e, TypeError) and (
                "NoneType" in error_message
                or "None" in error_message
                or any(
                    phrase in error_message
                    for phrase in [
                        "NoneType",
                        "object is None",
                        "'NoneType'",
                        "None has no attribute",
                        "unsupported operand type",
                        "bad operand type",
                        "object is not",
                        "cannot be None",
                    ]
                )
            ):
                return None
            else:
                raise

    return wrapper


class BaseSqlOperator:
    """Base class for individual SQL-based check operators (similar to BaseOperation)."""

    def __init__(self, data):
        self.original_data = data  # Store original data for creating other operators
        self.validation_df: DatasetInterface = data.get("df", PandasDataset(data=pd.DataFrame()))
        self.table_id: str = data["validation_dataset_id"]
        self.sql_data_service: PostgresQLDataService = data["sql_data_service"]
        self.column_prefix_map = data.get("column_prefix_map", {})
        self.value_level_metadata = data.get("value_level_metadata", [])
        self.column_codelist_map = data.get("column_codelist_map", {})
        self.codelist_term_maps = data.get("codelist_term_maps", [])
        self.operation_variables = data.get("operation_variables", {})

    @abstractmethod
    def execute_operator(self, other_value):
        """Execute the specific operator logic. Must be implemented by each operator."""
        pass

    def _assert_valid_value_and_cast(self, value):
        return value

    def _custom_str_conversion(self, x):
        if pd.notna(x):
            if isinstance(x, int):
                return str(x).strip()
            elif isinstance(x, float):
                return f"{x:.0f}" if x.is_integer() else str(x).strip()
        return x

    def convert_string_data_to_lower(self, data):
        if self.validation_df.is_series(data):
            data = data.str.lower()
        else:
            data = data.lower()
        return data

    def replace_prefix(self, value: str) -> Union[str, Any]:
        if isinstance(value, str):
            for prefix, replacement in self.column_prefix_map.items():
                if value.startswith(prefix) and replacement is not None:
                    return value.replace(prefix, replacement, 1)
        return value

    def replace_all_prefixes(self, values: List[str]) -> List[str]:
        for i in range(len(values)):
            values[i] = self.replace_prefix(values[i])
        return values

    def get_comparator_data(self, comparator, value_is_literal: bool = False):
        if value_is_literal:
            return comparator
        else:
            return self.validation_df.get(comparator, comparator)

    def _exists(self, column: str) -> bool:
        return self.sql_data_service.pgi.schema.column_exists(self.table_id, column)

    def _is_empty_sql(self, col: str) -> str:
        """
        Generates a SQL query to check if a column is empty.
        """
        column = self.sql_data_service.pgi.schema.get_column(self.table_id, col)
        if not column:
            raise ValueError(f"Column {col} does not exist in the table {self.table_id}.")

        match column.type:
            case "Char":
                return f"({column.hash} IS NULL OR {column.hash} = '')"
            case "Bool":
                return f"({column.hash} IS NULL)"
            case "Num":
                return f"({column.hash} IS NULL)"
            case _:
                raise ValueError(f"Unsupported column type: {column.type} for column {col}.")

    def _fetch_for_venmo(self, column: str):
        """
        Fetches data from a SQL table and returns it as a pandas Series,
        so we can pass it to Venmo.
        """
        # Fetch all of the rows
        self.sql_data_service.pgi.execute_sql(
            f"SELECT id, {self._column_sql(column)} as data FROM {self._table_sql()};"
        )
        sql_results = self.sql_data_service.pgi.fetch_all()

        # Fix off-by-one
        return_series = pd.Series(data={item["id"] - 1: item["data"] for item in sql_results})
        return return_series

    def _do_check_operator(self, new_column: str, sql_subquery_fn):
        # Handles simple checks by creating a column and updating it with a scalar subquery.
        exists = self.sql_data_service.pgi.schema.column_exists(self.table_id, new_column)
        if not exists:
            self.sql_data_service.pgi.add_column(
                table=self.table_id, schema=SqlColumnSchema.generated(new_column, "Bool")
            )

            subquery = sql_subquery_fn()
            query = f"UPDATE {self._table_sql()} SET {self._column_sql(new_column)} = ({subquery});"
            self.sql_data_service.pgi.execute_sql(query)
        return self._fetch_for_venmo(new_column)

    def _do_complex_check_operator(self, new_column: str, sql_full_query_fn):
        # Handles complex checks by creating a column and populating it with a full custom query.
        exists = self.sql_data_service.pgi.schema.column_exists(self.table_id, new_column)
        if not exists:
            self.sql_data_service.pgi.add_column(
                table=self.table_id, schema=SqlColumnSchema.generated(new_column, "Bool")
            )
            query = sql_full_query_fn(self._table_sql(), self._column_sql(new_column))
            self.sql_data_service.pgi.execute_sql(query)
        return self._fetch_for_venmo(new_column)

    def _table_sql(self):
        return self.sql_data_service.pgi.schema.get_table_hash(self.table_id)

    def _column_sql(self, column: str):
        return self.sql_data_service.pgi.schema.get_column_hash(self.table_id, column)

    def valid_codelist_reference(self, column_name, codelist):
        if column_name in self.column_codelist_map:
            return codelist in self.column_codelist_map[column_name]
        elif self.column_prefix_map:
            # Check for generic versions of variables (i.e --DECOD)
            for key in self.column_prefix_map:
                if column_name.startswith(self.column_prefix_map[key]):
                    generic_column_name = column_name.replace(self.column_prefix_map[key], key, 1)
                    if generic_column_name in self.column_codelist_map:
                        return codelist in self.column_codelist_map.get(generic_column_name)
        return True

    def _get_string_part_series(self, part_to_validate: str, length: int, target: str):
        """if not self.validation_df[target].apply(type).eq(str).all():
            raise ValueError("The operator can't be used with non-string values")

        if part_to_validate == "suffix":
            series_to_validate = self.validation_df[target].str.slice(-length)
        elif part_to_validate == "prefix":
            series_to_validate = self.validation_df[target].str.slice(stop=length)
        else:
            raise ValueError(
                f"Invalid part to validate: {part_to_validate}. \
                    Valid values are: suffix, prefix"
            )
        series_to_validate = series_to_validate.mask(pd.isna(self.validation_df[target]))
        return series_to_validate"""
        raise NotImplementedError("_get_string_part_series check_operator not implemented")

    def _check_equality_of_string_part(
        self,
        target: str,
        comparison_data,
        part_to_validate: str,
        length: int,
    ):
        """
        Checks if the given string part is equal to comparison data.
        """
        """series_to_validate = self._get_string_part_series(part_to_validate, length, target)
        return series_to_validate.eq(comparison_data).astype(bool)"""
        raise NotImplementedError("_check_equality_of_string_part check_operator not implemented")

    def compare_target_with_comparator_next_row(self, df: DatasetInterface, target: str, comparator: str):
        """
        Compares current row of a target with the next row of comparator.
        We can't
        compare last row of target with the next row of comparator
        because there is no row after the last one.
        """
        """target_without_last_row = df[target].drop(df[target].tail(1).index)
        comparator_without_first_row = df[comparator].drop(df[comparator].head(1).index)
        results = np.where(
            target_without_last_row.values == comparator_without_first_row.values,
            True,
            False,
        )
        # we add True at the end as the last row of target has nothing to compare
        # so as to not raise errors or incorrect issues in the report with False or NaN
        return self.validation_df.convert_to_series(
            [
                *results,
                True,
            ]
        ).tolist()"""
        raise NotImplementedError("compare_target_with_comparator_next_row check_operator not implemented")

    def next_column_exists_and_previous_is_null(self, row) -> bool:
        """row.reset_index(drop=True, inplace=True)
        for index in row[row.isin(NULL_FLAVORS) | pd.isna(row)].index:  # leaving null values only
            next_position: int = index + 1
            if next_position < len(row) and not (pd.isna(row[next_position]) or row[next_position] in NULL_FLAVORS):
                return True
        return False"""
        raise NotImplementedError("next_column_exists_and_previous_is_null check_operator not implemented")

    def _check_equality_literal(
        self,
        original_target,
        value,
        invert: bool = False,
        case_insensitive: bool = False,
        type_insensitive: bool = False,
    ):
        """
        Equality checks work slightly differently for clinical datasets.
        See truth table below:
        Operator       --A         --B         Outcome
        equal_to       "" or null  "" or null  False
        equal_to       "" or null  Populated   False
        equal_to       Populated   "" or null  False
        equal_to       Populated   Populated   A == B
        not_equal_to   "" or null  "" or null  False
        not_equal_to   "" or null  Populated   True
        not_equal_to   Populated   "" or null  True
        not_equal_to   Populated   Populated   A != B
        """
        target = original_target
        if case_insensitive:
            target = f"""LOWER({target})"""
            if isinstance(value, str):
                value = value.lower()

        if type_insensitive:
            target = f"""CAST({target} AS TEXT)"""

        def sql():
            if value is None or value == "":
                if invert:
                    query = f"""{original_target} IS NOT NULL AND {target} != ''"""
                else:
                    query = "FALSE"
            else:
                query = f"""{original_target} IS NOT NULL AND {target} = '{value}'"""
                if invert:
                    query = f"NOT ({query})"

            return query

        return self._do_check_operator(f"{original_target}={value}_{invert}_{case_insensitive}_{type_insensitive}", sql)

    def is_column_of_iterables(self, column):
        """return self.validation_df.is_series(column) and (
            isinstance(column.iloc[0], list) or isinstance(column.iloc[0], set)
        )"""
        raise NotImplementedError("is_column_of_iterables check_operator not implemented")

    def _check_equality_comparison(
        self,
        original_target,
        original_comparator,
        invert: bool = False,
        case_insensitive: bool = False,
        type_insensitive: bool = False,
    ):
        """
        Equality checks work slightly differently for clinical datasets.
        See truth table in _check_equality_literal for details.
        """
        target = original_target
        comparator = original_comparator
        if case_insensitive:
            target = f"""LOWER({target})"""
            comparator = f"""LOWER({comparator})"""

        if type_insensitive:
            target = f"""CAST({target} AS TEXT)"""
            comparator = f"""CAST({comparator} AS TEXT)"""

        def sql():
            if invert:
                return f"""CASE
                        WHEN {original_target} IS NULL OR {target} = ''
                            THEN {original_comparator} IS NULL OR {comparator} = ''
                        WHEN {original_comparator} IS NULL OR {comparator} = ''
                            THEN TRUE
                        ELSE {target} != {comparator}
                    END"""
            else:
                return f"""CASE
                        WHEN {original_target} IS NULL OR {target} = ''
                            THEN FALSE
                        WHEN {original_comparator} IS NULL OR {comparator} = ''
                            THEN FALSE
                        ELSE {target} = {comparator}
                    END"""

        return self._do_check_operator(
            f"{original_target}={original_comparator}_{invert}_{case_insensitive}_{type_insensitive}", sql
        )

    def _check_equality_reference(
        self,
        original_target,
        pivot_column,
        invert: bool = False,
        case_insensitive: bool = False,
        type_insensitive: bool = False,
    ):
        """
        Equality checks work slightly differently for clinical datasets.
        See truth table in _check_equality_literal for details.

        This method implements equality testing by reference, ie you specifiy a pivot
        column, that column is then used to look up which other column to compare
        that row against. The way we handle that in SQL is by finding out all of the
        columns that could be referenced (the DISTINCT values of the pivot column),
        and then generating a CASE statement that checks each of those values.
        """
        column = original_target

        # Find all of the values of the pivot column -> all columns to compare against
        self.sql_data_service.pgi.execute_sql(f"SELECT DISTINCT {pivot_column} col FROM {self.table_id};")
        comparison_values = self.sql_data_service.pgi.fetch_all()
        comparison_values = [item["col"].lower() for item in comparison_values]
        comparison_values = filter(self._exists, comparison_values)

        if case_insensitive:
            column = f"""LOWER({column})"""

        if type_insensitive:
            column = f"""CAST({column} AS TEXT)"""

        # This builds up the case statement for a simple column comparison
        def single_comparison_sql(original_c):
            c = original_c
            if case_insensitive:
                c = f"""LOWER({c})"""

            if type_insensitive:
                c = f"""CAST({c} AS TEXT)"""

            if invert:
                return f"""CASE
                        WHEN {original_target} IS NULL OR {column} = ''
                            THEN {original_c} IS NULL OR {c} = ''
                        WHEN {original_c} IS NULL OR {c} = ''
                            THEN TRUE
                        ELSE {column} != {c}
                    END"""
            else:
                return f"""CASE
                        WHEN {original_target} IS NULL OR {column} = ''
                            THEN FALSE
                        WHEN {original_c} IS NULL OR {c} = ''
                            THEN FALSE
                        ELSE {column} = {c}
                    END"""

        def sql():
            sql = "CASE "
            # Build a CASE statement for each possible column
            for c in comparison_values:
                sql += f"WHEN LOWER({pivot_column}) = '{c.lower()}' THEN ({single_comparison_sql(c)}) "
            sql += "ELSE FALSE END"
            return sql

        return self._do_check_operator(
            f"{original_target}_ref=_{pivot_column}_{invert}_{case_insensitive}_{type_insensitive}", sql
        )

    def _date_comparison(self, other_value: dict, operator: str):
        """
        Performs date comparison operations in PostgreSQL.
        Handles date component extraction and comparison.
        """
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        date_component = other_value.get("date_component")

        if isinstance(comparator, str) and not value_is_literal:
            comparator = self.replace_prefix(comparator).lower()

        component_suffix = f"_{date_component}" if date_component else ""
        cache_key = f"{target_column}{operator}{comparator}{component_suffix}"

        def sql():
            if date_component:
                component_map = {
                    "year": "YEAR",
                    "month": "MONTH",
                    "day": "DAY",
                    "hour": "HOUR",
                    "minute": "MINUTE",
                    "second": "SECOND",
                    "microsecond": "MICROSECONDS",
                }
                pg_component = component_map.get(date_component, "EPOCH")

                if value_is_literal:
                    return f"""CASE WHEN
                        {self.replace_prefix(target_column)} IS NOT NULL
                        AND {self.replace_prefix(target_column)} != ''
                        AND EXTRACT({pg_component} FROM CAST({self.replace_prefix(target_column)} AS TIMESTAMP))
                            {operator}
                        EXTRACT({pg_component} FROM CAST('{comparator}' AS TIMESTAMP))
                        THEN true
                        ELSE false
                        END"""
                else:
                    return f"""CASE WHEN
                        {self.replace_prefix(target_column)} IS NOT NULL
                        AND {self.replace_prefix(target_column)} != ''
                        AND {self.replace_prefix(comparator)} IS NOT NULL
                        AND {self.replace_prefix(comparator)} != ''
                        AND EXTRACT({pg_component} FROM CAST({self.replace_prefix(target_column)} AS TIMESTAMP))
                            {operator}
                        EXTRACT({pg_component} FROM CAST({self.replace_prefix(comparator)} AS TIMESTAMP))
                        THEN true
                        ELSE false
                        END"""
            else:
                if value_is_literal:
                    return f"""CASE WHEN
                        {target_column} IS NOT NULL
                        AND {target_column} != ''
                        AND CAST({target_column} AS TIMESTAMP)
                            {operator}
                        CAST('{comparator}' AS TIMESTAMP)
                        THEN true
                        ELSE false
                        END"""
                else:
                    return f"""CASE WHEN
                        {target_column} IS NOT NULL
                        AND {target_column} != ''
                        AND {comparator} IS NOT NULL
                        AND {comparator} != ''
                        AND CAST({target_column} AS TIMESTAMP)
                            {operator}
                        CAST({comparator} AS TIMESTAMP)
                        THEN true
                        ELSE false
                        END"""

        return self._do_check_operator(cache_key, sql)

    def _numeric_comparison(
        self,
        other_value: dict,
        operator: str,
    ):
        target_column = self.replace_prefix(other_value.get("target"))
        comparator = (
            other_value.get("comparator").lower()
            if isinstance(other_value.get("comparator"), str)
            else other_value.get("comparator")
        )

        def sql():
            return f"""CASE WHEN
                            CAST({target_column} AS NUMERIC)
                                {operator}
                            CAST({comparator} AS NUMERIC) THEN true
                        ELSE false
                        END
                        """

        return self._do_check_operator(f"{target_column}{operator}{comparator}", sql)

    def _series_is_in(self, target, comparison_data):
        return np.where(comparison_data.isin(target), True, False)
