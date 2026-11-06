from typing import List

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.util import NUMERIC_LITERAL_PATTERN
from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlSuppMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        supp: SqlTableSchema,
        domain: str,
    ) -> SqlTableSchema:
        """
        Adds the additional variables defined by the SUPP merge to the input dataset.

        The output table will have all of the columns from the left table as before.
        Any columns defined in QNAM will be added as columns to the output table.
        Any cells which do not have a corresponding record in SUPP will be set to NULL.
        """

        required_supp_columns = SqlSuppMerge._get_required_supp_columns(domain)
        required_original_columns = SqlSuppMerge._get_required_original_columns(pgi, supp, domain)
        new_columns = SqlSuppMerge._determine_new_columns(pgi, supp)

        SqlSuppMerge._validate_merge(original, supp, required_original_columns, required_supp_columns, new_columns)

        name = f"{original.name}_SUPP"
        schema = SqlTableSchema.derived(name, pgi)

        # Check if the table already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        # Build the new schema
        schema = SqlSuppMerge._build_schemas(name=name, original=original, new_columns=new_columns, pgi=pgi)

        pgi.create_table(schema)
        pgi.execute_sql(SqlSuppMerge._merge_query(schema, original, supp, required_original_columns, new_columns))

        return schema

    @staticmethod
    def _merge_query(
        schema: SqlTableSchema,
        original: SqlTableSchema,
        supp: SqlTableSchema,
        idvars: List[str],
        new_columns: List[str],
    ) -> str:
        """
        One pass over the original instead of a copy plus an UPDATE per QNAM per IDVAR
        (each of which rewrote every matching row).
        """

        def supp_alias(column: str) -> str:
            return f"s.{supp.get_column_hash(column)}"

        def original_alias(column: str) -> str:
            return f"o.{original.get_column_hash(column)}"

        copied = [(name, column) for name, column in original.get_columns() if name != "id" and not column.alias]
        targets = [schema.get_column_hash(name) for name, _ in copied]
        values = [f"o.{column.hash}" for _, column in copied]
        joins, pivots = [], []

        if new_columns:
            qvals = [
                f"MAX(CASE WHEN {supp_alias('QNAM')} = {_quote(qnam)} THEN {supp_alias('QVAL')} END) AS c{index}"
                for index, qnam in enumerate(new_columns)
            ]
            for index, idvar in enumerate(idvars or [None]):
                keys = [supp_alias("STUDYID"), supp_alias("RDOMAIN"), supp_alias("USUBJID")]
                on = [
                    f"{original_alias('STUDYID')} = p{index}.k0",
                    f"{original_alias('DOMAIN')} = p{index}.k1",
                    f"{original_alias('USUBJID')} = p{index}.k2",
                ]
                where = ""
                if idvar is not None:
                    keys.append(_link_key_sql(supp_alias("IDVARVAL")))
                    on.append(f"{_link_key_sql(original_alias(idvar))} = p{index}.k3")
                    where = f"WHERE {supp_alias('IDVAR')} = {_quote(idvar)}"
                key_select = ", ".join(f"{key} AS k{position}" for position, key in enumerate(keys))
                group_by = ", ".join(str(position) for position in range(1, len(keys) + 1))
                pivots.append(
                    f"p{index} AS (SELECT {key_select}, {', '.join(qvals)} "
                    f"FROM {supp.hash} s {where} GROUP BY {group_by})"
                )
                joins.append(f"LEFT JOIN p{index} ON {' AND '.join(on)}")

            last_first = list(reversed(range(len(pivots))))
            for index, qnam in enumerate(new_columns):
                targets.append(schema.get_column_hash(qnam))
                values.append(f"COALESCE({', '.join(f'p{link}.c{index}' for link in last_first)})")

        with_clause = f"WITH {', '.join(pivots)}" if pivots else ""
        return f"""
            {with_clause}
            INSERT INTO {schema.hash} ({', '.join(targets)})
                SELECT {', '.join(values)}
                FROM {original.hash} o
                {' '.join(joins)}
                ORDER BY o.id
        """

    @staticmethod
    def _build_schemas(
        name: str,
        original: SqlTableSchema,
        new_columns: List[str],
        pgi: PostgresQLInterface,
    ) -> SqlTableSchema:
        """Build the output schema."""

        joined_schema = SqlTableSchema.derived(name, pgi)

        # Add all of the original table's columns
        for name, column in original.get_columns():
            if name == "id":
                continue
            joined_schema.add_column(column)

        # Add the new columns
        for column in new_columns:
            if name == "id":
                continue

            # NOTE: All values in QVAL are Char, so all new columns will be Char type
            new_col_schema = SqlColumnSchema.generated(column=column, type="Char")
            joined_schema.add_column(new_col_schema)

        return joined_schema

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        supp: SqlTableSchema,
        required_original_columns: List[str],
        required_supp_columns: List[str],
        new_columns: List[str],
    ):
        """
        Validates the merge by checking if the original and supp schemas have the required columns.
        """
        for col in required_supp_columns:
            if not supp.has_column(col):
                raise ColumnNotFoundError(col, supp.dataset_name)

        for col in ["STUDYID", "DOMAIN", "USUBJID"]:
            if not original.has_column(col):
                raise ColumnNotFoundError(col, original.dataset_name)

        for col in required_original_columns:
            if not original.has_column(col):
                raise ValueError(f"SUPP MERGE: Original schema is missing required column: {col}")

        for col in new_columns:
            if original.has_column(col):
                raise ValueError(f"SUPP MERGE: Column already exists in original table: {col}")

    @staticmethod
    def _get_required_supp_columns(domain: bool) -> List[str]:
        """
        Returns the list of required columns in the supp dataset for the SUPP merge.
        SUPPDM does not require IDVAR or IDVARVAL, as USUBJID if already unique.
        """
        if domain == "DM":
            return ["STUDYID", "RDOMAIN", "USUBJID"]
        else:
            return ["STUDYID", "RDOMAIN", "USUBJID", "IDVAR", "IDVARVAL"]

    @staticmethod
    def _get_required_original_columns(pgi: PostgresQLInterface, supp: SqlTableSchema, domain: bool) -> List[str]:
        """
        Returns the list of required columns in the original dataset for the SUPP merge.
        DM does not require any extra columns as USUBJID if already unique.
        """
        if domain == "DM":
            return []
        else:
            if not supp.has_column("IDVAR"):
                raise ColumnNotFoundError("IDVAR", supp.dataset_name)

            pgi.execute_sql(f"SELECT DISTINCT {supp.get_column_hash("IDVAR")} AS col FROM {supp.hash}")
            result = pgi.fetch_all()
            return [row["col"] for row in result]

    @staticmethod
    def _determine_new_columns(pgi: PostgresQLInterface, supp_table: SqlTableSchema) -> List[str]:
        """
        Determines the new columns that will be added to the schema after the join.
        """
        pgi.execute_sql(f"SELECT DISTINCT {supp_table.get_column_hash("QNAM")} AS col FROM {supp_table.hash}")
        result = pgi.fetch_all()
        return [row["col"] for row in result]


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _link_key_sql(value_sql: str) -> str:
    """
    A join key equal on both sides exactly using a numeric-aware regex.
    """
    text = f"TRIM(CAST({value_sql} AS TEXT))"
    return (
        f"(CASE WHEN {text} ~ '{NUMERIC_LITERAL_PATTERN}' "
        f"THEN CAST(trim_scale(CAST({text} AS NUMERIC)) AS TEXT) "
        f"ELSE CAST({value_sql} AS TEXT) END)"
    )
