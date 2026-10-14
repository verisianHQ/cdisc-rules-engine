from typing import List, Optional

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VLM_TYPE,
)

SQL_TYPES = {"Bool": "BOOLEAN", "Num": "NUMERIC", "Char": "TEXT"}
NUMERIC_COMPARATORS = {"LT": "<", "LE": "<=", "GT": ">", "GE": ">=", "EQ": "=", "NE": "<>"}
ROW_NUMBER_ALIAS = "vlm_source_row_number"


class SqlValueCheckAgainstDefineVLMDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    that is described by Define XML value level metadata is a row in the new dataset.
    A value is described by a VLM when the record matches the VLM's where clause.
    Only variable values that have VLM attached are produced.

    Columns available in the dataset are:
        row_number, variable_name, variable_value, define_variable_name, define_vlm_...
    plus all columns of the source record, so a rule can check other variables of the record
    that the VLM applies to.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_value_check_define_vlm"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        vlm_columns = {
            "row_number": "Num",
            "variable_name": "Char",
            "variable_value": "Char",
            "define_variable_name": "Char",
            **DEFINE_VLM_TYPE,
        }
        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in vlm_columns.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        source_columns = [
            (name, sc)
            for name, sc in source_schema.get_columns()
            if name.lower() != "id" and sc.origin == "data" and name.lower() not in vlm_columns
        ]
        for name, sc in source_columns:
            schema.add_column(SqlColumnSchema.generated(name, sc.type))

        self.data_service.pgi.create_table(schema)

        select_statements = []
        for vlm in self.get_define_vlms():
            variable_name = (vlm.get("define_variable_name") or "").upper()
            value_hash = source_schema.get_column_hash(variable_name)
            where = self._where_clause_to_sql(vlm.get("where_clause"), source_schema)
            if not value_hash or not where:
                continue

            select_parts = [
                ROW_NUMBER_ALIAS,
                f"CAST('{variable_name}' AS TEXT)",
                f"CAST(src.{value_hash} AS TEXT)",
                f"CAST('{variable_name}' AS TEXT)",
            ]
            for key, type in DEFINE_VLM_TYPE.items():
                select_parts.append(self._literal(vlm.get(key), type))
            select_parts += [f"src.{sc.hash}" for _, sc in source_columns]

            select_statements.append(
                f"SELECT {', '.join(select_parts)} "
                f"FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS {ROW_NUMBER_ALIAS} "
                f"FROM {source_table_hash}) src WHERE {where}"
            )

        if select_statements:
            target_columns = [schema.get_column_hash(col) for col in vlm_columns] + [
                schema.get_column_hash(name) for name, _ in source_columns
            ]
            insert_query = (
                f"INSERT INTO {schema.hash} ({', '.join(target_columns)}) " f"{' UNION ALL '.join(select_statements)};"
            )
            self.data_service.pgi.execute_sql(insert_query)

        return table_name

    @staticmethod
    def _literal(value, type: str) -> str:
        sql_type = SQL_TYPES.get(type, "TEXT")
        val = str(value).replace("'", "''").strip()
        if value is None or val.lower() in ["", "none", "null", "''", '""']:
            return f"CAST(NULL AS {sql_type})"
        return f"CAST('{val}' AS {sql_type})"

    @classmethod
    def _where_clause_to_sql(cls, where_clause: List[dict], source_schema: SqlTableSchema) -> Optional[str]:
        if not where_clause:
            return None
        conditions = []
        for check in where_clause:
            column = source_schema.get_column(check["variable"])
            if column is None:
                return None
            condition = cls._range_check_to_sql(f"src.{column.hash}", column.type, check["comparator"], check["values"])
            if condition is None:
                return None
            conditions.append(condition)
        return " AND ".join(f"({condition})" for condition in conditions)

    @staticmethod
    def _range_check_to_sql(column: str, column_type: str, comparator: str, values: List[str]) -> Optional[str]:
        if not values:
            return None
        numeric_values = []
        if column_type == "Num":
            try:
                numeric_values = [str(float(value)) for value in values]
            except ValueError:
                pass

        if numeric_values:
            target = column
            literals = numeric_values
        else:
            target = f"COALESCE(CAST({column} AS TEXT), '')"
            literals = ["'" + str(value).replace("'", "''") + "'" for value in values]

        if comparator in NUMERIC_COMPARATORS:
            return f"{target} {NUMERIC_COMPARATORS[comparator]} {literals[0]}"
        if comparator == "IN":
            return f"{target} IN ({', '.join(literals)})"
        if comparator == "NOTIN":
            return f"{target} NOT IN ({', '.join(literals)})"
        return None
