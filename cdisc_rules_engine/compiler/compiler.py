"""
Turns a rule + a dataset into a SELECT query.

Roughly:

    SELECT <columns the report needs>, <operation outputs> <- {output variables} + {metadata for results}
    FROM <rule source, plus window columns> co
         LEFT JOIN <whatever the checks pre-aggregate>
    WHERE <checks>
    ORDER BY co.id [LIMIT ...]

The rule's sensitivity is handled in SQL too (first row per dataset / per group, or a capped
set of records), so only rows that will actually be reported come back to python.
This is how we avoid the crazy pandas Series column return type when fetching results.
Huzzah, memory savings!
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from cdisc_rules_engine.compiler.conditions import CO, CompiledConditions, ConditionCompiler
from cdisc_rules_engine.compiler.targets import get_target_columns
from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME, SOURCE_DS, SOURCE_ROW_NUMBER, TOTAL_COUNT_COLUMN
from cdisc_rules_engine.constants.rule_constants import MAX_ERRORS_PER_DATASET
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult

FILTER_BY_DATASET = "filter_by_dataset"
REPORTED = "reported"  # stops us from re-selecting already reported rows
SEMI = "semi"
PART_ROW_COLUMN = "__part_row"
SUMMARY_SENSITIVITIES = {Sensitivity.DATASET.value, Sensitivity.STUDY.value}
ROW_SENSITIVITIES = {Sensitivity.RECORD.value, Sensitivity.GROUP.value, None}
NULL_KEY = "CAST(NULL AS TEXT)"  # used as the group key when there's nothing to group on or the column is missing


@dataclass
class CompiledQuery:
    sql: str
    schema: SqlTableSchema
    conditions: CompiledConditions
    target_columns: Dict[str, bool] = field(default_factory=dict)
    inline_outputs: List[str] = field(default_factory=list)


class RuleQueryCompiler:
    """
    Compiles a rule and dataset into a SQL SELECT query.
    Helpful categories! Yay!
    """

    def __init__(
        self,
        rule: dict,
        data_service,
        dataset_metadata,
        dataset_id: str,
        operation_variables: Optional[Dict[str, SqlOperationResult]] = None,
        target_rule: Optional[dict] = None,
        max_errors: int = MAX_ERRORS_PER_DATASET,
    ):
        self.rule = rule
        self.max_errors = max_errors
        self.target_rule = target_rule or rule
        self.data_service = data_service
        self.pgi = data_service.pgi
        self.dataset_metadata = dataset_metadata
        self.dataset_id = dataset_id
        self.operation_variables = operation_variables or {}
        self.domain = (dataset_metadata.domain or "") if dataset_metadata else ""
        self.schema: SqlTableSchema = self.pgi.schema.get_table(dataset_id)

    def compile(self, inline_outputs: bool = True) -> CompiledQuery:
        conditions = ConditionCompiler(
            self.data_service, self.dataset_id, self.dataset_metadata, self.operation_variables
        ).compile(self.rule["conditions"])
        target_columns = get_target_columns(self.target_rule, self.dataset_metadata, self.schema)

        operation_outputs = [
            name
            for name, present in target_columns.items()
            if present and name.startswith("$") and name in self.operation_variables
        ]
        inlined = operation_outputs if inline_outputs else []

        columns = self._select_hashes(target_columns, operation_outputs)
        join = self._semi_join(conditions)
        if join is None:
            query = self._joined_query(conditions, columns)
        else:
            query = self._semi_join_query(join, conditions.where, columns)

        sql = self._with_operation_outputs(query, inlined)
        return CompiledQuery(sql, self.schema, conditions, target_columns, inlined)

    # --- SELECT list ---------------------------------------------------------------------

    def _select_hashes(self, target_columns: Dict[str, bool], operation_outputs: List[str]) -> List[str]:
        wanted = [
            "usubjid",
            f"{self.domain}SEQ",
            SOURCE_ROW_NUMBER,
            SOURCE_DS,
            DATASET_NAME,
            *[column for column, present in target_columns.items() if present and not column.startswith("$")],
            *self._grouping_columns(),
            # an operation output is computed from the reported row's columns (in the query, or per row by the
            # result handler when the query can't), so they have to be selected either way
            *[
                column
                for name in operation_outputs
                for column in (self.operation_variables[name].params or {}).values()
            ],
        ]
        hashes = []
        for column in wanted:
            column_hash = self.schema.get_column_hash(column)
            # missing columns are just skipped
            # the handler reports them as "Not in dataset"
            if column_hash and column_hash != "id" and column_hash not in hashes:
                hashes.append(column_hash)
        return hashes

    def _joined_query(self, conditions: CompiledConditions, columns: List[str]) -> str:
        select = _select(columns)
        source = conditions.from_clause(self.schema.hash)
        return self._query(select, source, conditions.where)

    def _semi_join(self, conditions: CompiledConditions):
        join = self.schema.join_source
        if join is None:
            return None
        if conditions.derived_columns or conditions.joins:
            return None

        is_grouped = self.rule.get("sensitivity") == Sensitivity.GROUP.value
        if is_grouped and self._grouping_variables():
            return None

        if _realiases_source(conditions.where):
            return None

        where = _right_side(conditions.where, join)
        if _mentions_right_columns(where, join):
            return None
        return join

    def _semi_join_query(self, join, where: str, columns: List[str]) -> str:
        where = _right_side(where, join)
        right_hashes = {joined for _, joined in join.right_columns}

        left_columns = [column for column in columns if column not in right_hashes]
        right_columns = [(column, joined) for column, joined in join.right_columns if joined in columns]

        select = _select(left_columns)
        source = f"{join.left.hash} {CO}"
        exists = f"EXISTS ({_failing_right_rows(join, where, '1')})"
        query = self._query(select, source, exists)

        if not right_columns:
            return query
        return _with_right_values(query, join, where, right_columns)

    # --- sensitivity ---------------------------------------------------------------------

    def _query(self, select: str, source: str, where: str) -> str:
        sensitivity = self.rule.get("sensitivity")
        if sensitivity in SUMMARY_SENSITIVITIES:
            return self._summary_query(select, source, where)
        if sensitivity == Sensitivity.GROUP.value and self._grouping_variables():
            return self._group_query(select, source, where)
        if sensitivity not in ROW_SENSITIVITIES:
            raise ValueError(f"Invalid sensitivity value: {sensitivity}")
        # Record and Group with no grouping variables, which behaves like Record
        return self._capped(select, source, where, CO)

    def _summary_query(self, select: str, source: str, where: str) -> str:
        """Dataset / Study: one error so just the first failing row."""
        if self._is_split() and self.schema.has_column(SOURCE_DS):
            # ...except a concatenated split dataset which gets one failing row per source file
            part = f"LOWER(COALESCE(CAST({CO}.{self.schema.get_column_hash(SOURCE_DS)} AS TEXT), ''))"
            return _first_per_group(select, source, where, [part])
        return f"SELECT {select} FROM {source} WHERE {where} ORDER BY {CO}.id LIMIT 1"

    def _group_query(self, select: str, source: str, where: str) -> str:
        """Group: first failing row of each group, capped like record errors."""
        filters = [where]
        if FILTER_BY_DATASET in self._grouping_variables():
            filters.append(self._same_dataset_sql())
        # If filter_by_dataset was the only "grouping variable" everything lands in one group
        keys = [self._grouping_key_sql(column) for column in self._grouping_columns()] or [NULL_KEY]
        first_rows = _first_per_group(select, source, " AND ".join(f"({f})" for f in filters), keys)
        return self._capped("first_rows.*", f"({first_rows}) first_rows", "TRUE", "first_rows")

    def _capped(self, select_sql: str, source: str, where: str, alias: str) -> str:
        """
        At most max_errors rows.
        COUNT(*) OVER () is evaluated before the cap, so every row also
        carries the uncapped total, which is where "(50000 of 812345 errors shown)" comes from.
        """
        order_column = f"{alias}.id"
        part = self._split_part_sql(alias)
        if part is None:
            # MATERIALIZED stops Postgres planning the filter for the LIMIT, which picks a nested loop on large datasets
            return (
                f"WITH capped_rows AS MATERIALIZED (SELECT {select_sql} FROM {source} WHERE {where}) "
                f'SELECT *, COUNT(*) OVER () AS "{TOTAL_COUNT_COLUMN}" FROM capped_rows '
                f"ORDER BY capped_rows.id LIMIT {self.max_errors}"
            )
        return (
            f'SELECT * FROM (SELECT {select_sql}, COUNT(*) OVER (PARTITION BY {part}) AS "{TOTAL_COUNT_COLUMN}", '
            f'ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {order_column}) AS "{PART_ROW_COLUMN}" '
            f'FROM {source} WHERE {where}) capped WHERE capped."{PART_ROW_COLUMN}" <= {self.max_errors} '
            f"ORDER BY capped.id"
        )

    def _split_part_sql(self, alias: str) -> Optional[str]:
        """The source file of a row of a concatenated split dataset, as rows are told apart when reported."""
        if not (self._is_split() and self.schema.has_column(SOURCE_DS)):
            return None
        column_hash = self.schema.get_column_hash(SOURCE_DS)
        column = f"{alias}.{column_hash}" if alias == CO else f'{alias}."{column_hash}"'
        return f"LOWER(COALESCE(CAST({column} AS TEXT), ''))"

    def _grouping_variables(self) -> List[str]:
        return [variable for variable in self.rule.get("grouping_variables") or [] if isinstance(variable, str)]

    def _grouping_columns(self) -> List[str]:
        """Grouping variables that are real cols with the wildcard as domain."""
        return [
            variable.replace("--", self.domain)
            for variable in self._grouping_variables()
            if variable != FILTER_BY_DATASET
        ]

    def _grouping_key_sql(self, variable: str) -> str:
        """A grouping variable the dataset doesn't have is the same (missing) value on every row"""
        column_hash = self.schema.get_column_hash(variable)
        return f"{CO}.{column_hash}" if column_hash else NULL_KEY

    def _same_dataset_sql(self) -> str:
        column_hash = self.schema.get_column_hash(DATASET_NAME)
        if not column_hash:
            return "FALSE"
        return f"{CO}.{column_hash} = {_quote(self.dataset_metadata.name)}"

    def _is_split(self) -> bool:
        return bool(getattr(self.dataset_metadata, "split_part_filenames", None))

    # --- operation outputs ---------------------------------------------------------------

    def _with_operation_outputs(self, query: str, inlined: List[str]) -> str:
        """
        Wrap the chosen rows and compute operation outputs for just those rows.
        Doing it after the sensitivity/LIMIT step matters btw.
        A correlated subquery per reported row is fine, one per row of the whole dataset is not.
        Grouped operations get a single LEFT JOIN against their per-group values.
        Anything else becomes a scalar subquery per row.
        Messy I know.
        """
        if not inlined:
            return query
        outputs, joins = [], []
        for index, name in enumerate(inlined):
            result = self.operation_variables[name]
            group_join = self._group_join(result, f"output_{index}")
            if group_join is None:
                outputs.append(f'{self._operation_output_sql(result)} AS "{name}"')
                continue
            join, value = group_join
            joins.append(join)
            outputs.append(f'{value} AS "{name}"')
        return (
            f"SELECT {REPORTED}.*, {', '.join(outputs)} FROM ({query}) {REPORTED} {' '.join(joins)} "
            f"ORDER BY {REPORTED}.id"
        )

    def _group_join(self, result: SqlOperationResult, alias: str):
        """
        (join, value expression) for a grouped operation, or None if it can't be joined
        e.g. it isn't grouped OR its params don't line up 1:1 with param $1, $2, ... group columns.
        """
        params = result.params or {}
        grouping = result.group_by_columns
        if result.group_by_query is None or not grouping or len(params) != len(grouping):
            return None
        if not all(re.fullmatch(r"\$\d+", placeholder) for placeholder in params):
            return None

        conditions = []
        for placeholder, column in params.items():
            group_column = f"{alias}.group_{int(placeholder[1:])}"
            row_value = self._reported_column_sql(column)
            # NULL group matches NULL row value. This is IS NOT DISTINCT FROM spelled out: Postgres
            # can't hash/merge join on IS NOT DISTINCT FROM, but it can on the COALESCE equality,
            # and the IS NULL comparison stops NULL from matching ''.
            conditions.append(
                f"({group_column} IS NULL) = ({row_value} IS NULL) "
                f"AND COALESCE(CAST({group_column} AS TEXT), '') = COALESCE(CAST({row_value} AS TEXT), '')"
            )
        join = f"LEFT JOIN ({result.group_by_query}) AS {alias} ON {' AND '.join(conditions)}"
        return join, result.group_value_sql.format(value=f"{alias}.value")

    def _reported_column_sql(self, column: str) -> str:
        if column == "id":
            return f"{REPORTED}.id"
        column_hash = self.schema.get_column_hash(column)
        return f'{REPORTED}."{column_hash}"' if column_hash else "NULL"

    def _operation_output_sql(self, result: SqlOperationResult) -> str:
        query = self._bind_parameters(result)
        if result.type == "collection":
            return f"ARRAY(SELECT op.value FROM ({query}) AS op)"
        # constant: first value wins
        return f"(SELECT * FROM ({query}) AS op LIMIT 1)"

    def _bind_parameters(self, result: SqlOperationResult) -> str:
        query = result.query
        # Longest placeholder first, otherwise replacing $1 would also eat the start of $10
        # TODO: proper bind params would avoid string replacement entirely
        for placeholder, column in sorted((result.params or {}).items(), key=lambda item: -len(item[0])):
            query = query.replace(placeholder, self._reported_column_sql(column))
        return query


def _select(columns: List[str]) -> str:
    items = [f"{CO}.id AS id"]
    for column in columns:
        items.append(f'{CO}.{column} AS "{column}"')
    return ", ".join(items)


def _right_side(where: str, join) -> str:
    for column, joined in join.right_columns:
        pattern = rf"\b{CO}\.{re.escape(joined)}\b"
        where = re.sub(pattern, f"r.{column}", where)
    return where


def _realiases_source(where: str) -> bool:
    return re.search(rf"\bAS\s+{CO}\b", where, re.IGNORECASE) is not None


def _mentions_right_columns(where: str, join) -> bool:
    for _, joined in join.right_columns:
        if joined in where:
            return True
    return False


def _failing_right_rows(join, where: str, select: str) -> str:
    keys = join.on(CO, "r")
    return f"SELECT {select} FROM {join.right.hash} r WHERE {keys} AND ({where})"


def _with_right_values(query: str, join, where: str, right_columns: List[tuple]) -> str:
    values = []
    for column, joined in right_columns:
        values.append(f'r.{column} AS "{joined}"')
    first_failing = _failing_right_rows(join, where, ", ".join(values)) + " ORDER BY r.id LIMIT 1"

    return (
        f"SELECT {SEMI}.*, matched.* FROM ({query}) {SEMI} "
        f"JOIN {join.left.hash} {CO} ON {CO}.id = {SEMI}.id "
        f"LEFT JOIN LATERAL ({first_failing}) matched ON TRUE "
        f"ORDER BY {SEMI}.id"
    )


def _first_per_group(select_sql: str, source: str, where: str, keys: List[str]) -> str:
    # DISTINCT ON needs ORDER BY to lead with the keys so re-sort by id outside to keep row order
    key_sql = ", ".join(keys)
    return (
        f"SELECT * FROM (SELECT DISTINCT ON ({key_sql}) {select_sql} FROM {source} WHERE {where} "
        f"ORDER BY {key_sql}, {CO}.id) first_per_group ORDER BY first_per_group.id"
    )


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"
