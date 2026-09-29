from abc import abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import RuleExecutionError
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

PREVIOUS_OPERATION_ALIAS = "inner_op"


@dataclass
class AggregateSource:
    from_sql: str
    value_sql: Optional[str]
    subtype: Optional[str]
    resolve_column: Callable[[str], Optional[Tuple[str, str]]]
    previous_operation_name: Optional[str] = None


class SqlAggregateOperation(SqlBaseOperation):
    """
    Base for operations that aggregate values into one.
    This includes max, min, record_count, mean, max_date and min_date.
    """

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, function: str):
        super().__init__(params, data_service)
        self.function = function

    @abstractmethod
    def _dataset_value(self, table: str) -> Tuple[Optional[str], Optional[str]]:
        """The SQL for the values to aggregate from the dataset and their subtype."""

    @abstractmethod
    def _aggregate_sql(self, value_sql: str, aggregate_filter: str = "") -> str:
        """The aggregate expression selected as the operation's value."""

    def _group_aggregate_sql(self, value_sql: str, aggregate_filter: str) -> str:
        """The aggregate expression selected as each group's value in group_by_query."""
        return self._aggregate_sql(value_sql, aggregate_filter)

    def _result_subtype(self, source: AggregateSource) -> Optional[str]:
        return source.subtype

    def _execute_operation(self):
        source = self._source()
        conditions = self._conditions(source)
        subtype = self._result_subtype(source)

        if not self.params.grouping:
            query = f"SELECT {self._aggregate_sql(source.value_sql)} AS value FROM {source.from_sql}"
            return SqlOperationResult(query=self._with_where(query, conditions), type="constant", subtype=subtype)

        grouping = []
        for name in self.params.grouping:
            column = self._resolve_column(source, name, "group")
            if column is None:
                raise ValueError(f"Grouping column '{name}' not found in '{self._table()}'")
            grouping.append(column)
        group_by_query, group_by_columns = self._group_by_query(source, conditions, grouping)

        params = {}
        row_conditions = list(conditions)
        for i, (name, column_sql) in enumerate(grouping):
            param_name = f"${i + 1}"
            row_conditions.append(f"({column_sql} = {param_name} OR ({column_sql} IS NULL AND {param_name} IS NULL))")
            params[param_name] = name

        query = f"SELECT {self._aggregate_sql(source.value_sql)} AS value FROM {source.from_sql}"
        return SqlOperationResult(
            query=self._with_where(query, row_conditions),
            type="constant",
            subtype=subtype,
            params=params,
            group_by_query=group_by_query,
            group_by_columns=group_by_columns,
        )

    def _source(self) -> AggregateSource:
        previous_operation = (self.params.previous_operations or {}).get(self.params.target)
        if previous_operation is not None:
            return self._previous_operation_source(previous_operation)

        table = self._table()
        schema = self.data_service.pgi.schema

        def resolve_column(name: str) -> Optional[Tuple[str, str]]:
            column = schema.get_column(table, name)
            return (column.name, column.hash) if column else None

        value_sql, subtype = self._dataset_value(table)
        return AggregateSource(
            from_sql=schema.get_table_hash(table),
            value_sql=value_sql,
            subtype=subtype,
            resolve_column=resolve_column,
        )

    def _table(self) -> str:
        return self.params.table if self.params.use_rule_type_table else self.params.domain

    def _previous_operation_source(self, previous_operation: SqlOperationResult) -> AggregateSource:
        if previous_operation.group_by_query is None:
            raise RuleExecutionError(
                f"Operation {self.function} can only reference a previous operation that "
                f"was itself grouped, but {self.params.target} has no grouped result to "
                f"aggregate over."
            )

        group_by_columns = previous_operation.group_by_columns or {}

        def resolve_column(name: str) -> Optional[Tuple[str, str]]:
            alias = group_by_columns.get(name.lower())
            return (name.lower(), f"{PREVIOUS_OPERATION_ALIAS}.{alias}") if alias else None

        return AggregateSource(
            from_sql=f"({previous_operation.group_by_query}) AS {PREVIOUS_OPERATION_ALIAS}",
            value_sql=f"{PREVIOUS_OPERATION_ALIAS}.value",
            subtype=previous_operation.subtype,
            resolve_column=resolve_column,
            previous_operation_name=self.params.target,
        )

    def _resolve_column(self, source: AggregateSource, name: str, usage: str) -> Optional[Tuple[str, str]]:
        column = source.resolve_column(name)
        if column is None and source.previous_operation_name is not None:
            raise RuleExecutionError(
                f"Operation {self.function} can only {usage} by the grouping columns of "
                f"{source.previous_operation_name}, but {name} is not one of them."
            )
        return column

    def _filter_column_sql(self, source: AggregateSource, name: str) -> Optional[str]:
        column = self._resolve_column(source, name, "filter")
        return column[1] if column else None

    def _conditions(self, source: AggregateSource) -> List[str]:
        conditions = []
        filter_clause = self.construct_where_clause(resolve_column=lambda name: self._filter_column_sql(source, name))
        if filter_clause:
            conditions.append(filter_clause.removeprefix("WHERE "))
        if self.params.regex and source.value_sql != "*":
            pattern = self.params.regex.replace("'", "''")
            conditions.append(f"{source.value_sql}::text ~ '{pattern}'")
        return conditions

    def _group_by_query(
        self, source: AggregateSource, conditions: List[str], grouping: List[Tuple[str, str]]
    ) -> Tuple[str, dict]:
        aggregate_filter, group_where = "", []
        if conditions:
            if self.params.ignore_empty_filtered_groups:
                group_where = conditions
            else:
                aggregate_filter = f" FILTER (WHERE {' AND '.join(conditions)})"

        group_by_columns = {name: f"group_{i + 1}" for i, (name, _) in enumerate(grouping)}
        selected_columns = "".join(f", {column_sql} AS {group_by_columns[name]}" for name, column_sql in grouping)
        query = (
            f"SELECT {self._group_aggregate_sql(source.value_sql, aggregate_filter)} AS value{selected_columns} "
            f"FROM {source.from_sql}"
        )
        query = self._with_where(query, group_where)
        query += f" GROUP BY {', '.join(column_sql for _, column_sql in grouping)}"
        return query, group_by_columns

    @staticmethod
    def _with_where(query: str, conditions: List[str]) -> str:
        return f"{query} WHERE {' AND '.join(conditions)}" if conditions else query
